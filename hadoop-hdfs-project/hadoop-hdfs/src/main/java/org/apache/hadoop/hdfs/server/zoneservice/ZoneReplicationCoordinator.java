/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.zoneservice;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.zoneservice.metrics.ZoneProgressTracker;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The main functions of this class:
 * 1. Limit the speed of inter-dc block copy scheduled by namenode
 * 2. Wait for namenode to add/remove replicas
 */
public class ZoneReplicationCoordinator {

  private final DFSClient dfs;
  private static final long SLEEP_PERIOD = 1000L;
  private final AtomicBoolean isWaitingCompletion = new AtomicBoolean(false);
  private final BlockingQueue<FileState> waitFiles;
  private final BlockingQueue<FileState> finishedFiles;
  private final int maxConcurrentReplications;
  private final AtomicInteger runningReplications = new AtomicInteger(0);
  private final AtomicInteger runningDeletions = new AtomicInteger(0);
  private final long minCheckInterval;
  private final int maxCheckTimes;
  private final Thread checker = new Thread(new Checker());
  private static final Logger LOG =
      LoggerFactory.getLogger(ZoneReplicationCoordinator.class);

  ZoneReplicationCoordinator(Configuration conf, DistributedFileSystem fs) {
    this.dfs = fs.getClient();
    this.maxConcurrentReplications = conf.getInt(
        DFSConfigKeys.DFS_ZONE_COORDINATOR_MAX_CONCURRENT_REPLICATIONS_KEY,
        DFSConfigKeys.DFS_ZONE_COORDINATOR_MAX_CONCURRENT_REPLICATIONS_DEFAULT);
    this.waitFiles = new LinkedBlockingQueue<>(10000);
    this.finishedFiles = new LinkedBlockingQueue<>(10000);
    this.minCheckInterval = conf.getLong(
        DFSConfigKeys.DFS_ZONE_COORDINATOR_MIN_CHECK_INTERVAL_KEY,
        DFSConfigKeys.DFS_ZONE_COORDINATOR_MIN_CHECK_INTERVAL_DEFAULT);
    this.maxCheckTimes = conf.getInt(
        DFSConfigKeys.DFS_ZONE_COORDINATOR_MAX_CHECK_TIMES_KEY,
        DFSConfigKeys.DFS_ZONE_COORDINATOR_MAX_CHECK_TIMES_DEFAULT);
    checker.start();
  }

  /**
   * Add a file to the queue.
   * @param filePath full path of the file
   * @param blockReplicaDelta the number of replicas to add
   * @param blockNum number of blocks
   */
  public void addFile(String filePath, ReplicationRule rule,
      int blockReplicaDelta, int blockNum) {
    addFile(filePath, HdfsConstants.INVALIDATE_INODE_ID, rule, blockReplicaDelta, blockNum);
  }

  public void addFile(String filePath, long fileId, ReplicationRule rule,
      int blockReplicaDelta, int blockNum) {
    if (isWaitingCompletion.get()) {
      throw new UnsupportedOperationException(
          "Cannot add new files while waiting completion!");
    }
    Preconditions.checkNotNull(filePath);
    Preconditions.checkNotNull(rule);

    if (blockNum <= 0 || blockReplicaDelta == 0) {
      return;
    }

    int replicaDelta = blockReplicaDelta * blockNum;
    if (replicaDelta < 0) {
      int n = runningDeletions.addAndGet(-replicaDelta);
      LOG.debug("Added {} deletions, now runningDeletions is: {}", -replicaDelta, n);
    } else {
      while (runningReplications.get() + replicaDelta > maxConcurrentReplications) {
        try {
          LOG.debug("Waiting runningReplications to have enough quota for {} ...", filePath);
          //noinspection BusyWait
          Thread.sleep(SLEEP_PERIOD);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      int n = runningReplications.addAndGet(replicaDelta);
      LOG.debug("Added {} replicas, now runningReplications is: {}", replicaDelta, n);
    }
    boolean successOffered = waitFiles.offer(new FileState(filePath, fileId, rule, maxCheckTimes,
        replicaDelta));
    if (!successOffered) {
      LOG.info("addFile failed to offer the file to waitFiles." +
          "The size of waitFiles is {}.", waitFiles.size());
      try {
        waitFiles.put(new FileState(filePath, fileId, rule, maxCheckTimes, replicaDelta));
      } catch (InterruptedException e) {
        LOG.error("Failed to put fileState to waitFiles", e);
      }
    }
  }

  /**
   * Block until finish all files.
   */
  public void waitForCheckCompletion() {
    ZoneProgressTracker.startCountingCoordinatorWaitTime();
    this.isWaitingCompletion.set(true);
    try {
      checker.join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      ZoneProgressTracker.finishCountingCoordinatorWaitTimeAndLog();
    }
  }

  /**
   * Get one finished file from the finished queue.
   * @return FileState
   * @throws NoSuchElementException if no more files
   */
  public FileState getNextFinishedFile() throws NoSuchElementException {
    return getNextFinishedFile(Integer.MAX_VALUE);
  }

  /**
   * Get one finished file from the finished queue.
   * @param timeout time in milliseconds
   * @return FileState or null if timeout
   * @throws NoSuchElementException if no more files
   */
  public FileState getNextFinishedFile(long timeout) throws NoSuchElementException {
    if (finishedFiles.size() > 0) {
      return finishedFiles.poll();
    }
    long endTime = Time.monotonicNow() + timeout;
    while (!isWaitingCompletion.get() ||
        runningReplications.get() > 0 ||
        runningDeletions.get() > 0) {
      try {
        //noinspection BusyWait
        Thread.sleep(SLEEP_PERIOD);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      if (finishedFiles.size() > 0) {
        return finishedFiles.poll();
      } else if (Time.monotonicNow() > endTime) {
        return null;
      }
    }
    throw new NoSuchElementException("No more files!");
  }

  /**
   * The utility to check if a file has correct number of replicas.
   */
  class Checker implements Runnable {
    @Override
    public void run() {
      while (true) {
        if (!waitFiles.isEmpty()) {
          FileState fileState = waitFiles.poll();
          if (fileState == null) {
            continue;
          }

          long elapsed = Time.monotonicNow() - fileState.lastCheckTime;
          if (elapsed < minCheckInterval) {
            try {
              //noinspection BusyWait
              Thread.sleep(minCheckInterval - elapsed);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          }

          HdfsLocatedFileStatus status = getFileStatus(fileState);
          int replicaDelta = fileState.getReplicaDelta();
          if (status == null) {
            minusReplicaDeltaFromRunning(replicaDelta);
            continue;
          }

          fileState.setFileStatus(status);
          if (areAllBlocksHaveCorrectReplicas(fileState)) {
            boolean successOffered = finishedFiles.offer(fileState);
            if (!successOffered) {
              LOG.info("Checker failed to offer the file to finishedFiles." +
                  "the size of finishedFiles is {}.", finishedFiles.size());
              try {
                finishedFiles.put(fileState);
              } catch (InterruptedException e) {
                LOG.error("Failed to put fileState to finishedFiles", e);
              }
            }
            minusReplicaDeltaFromRunning(replicaDelta);
          } else {
            int leftCheckTimes = fileState.getLeftCheckTimes() - 1;
            if (leftCheckTimes <= 0) {
              LOG.warn("This file({}) does not have correct replicas after checking {} times!",
                  fileState.getFilePath(), maxCheckTimes);
              minusReplicaDeltaFromRunning(replicaDelta);
            } else {
              LOG.info("This file({}) does not have correct replicas with leftCheckTimes={}",
                  fileState.filePath, leftCheckTimes);
              fileState.setLeftCheckTimes(leftCheckTimes);
              fileState.setLastCheckTime(Time.monotonicNow());
              boolean successOffered = waitFiles.offer(fileState);
              if (!successOffered) {
                LOG.info("Checker failed to offer the file to waitFiles." +
                    "the size of waitFiles is {}.", waitFiles.size());
                try {
                  waitFiles.put(fileState);
                } catch (InterruptedException e) {
                  LOG.error("Failed to put fileState to waitFiles.", e);
                }
              }
            }
          }
        } else if (!isWaitingCompletion.get()) {
          try {
            //noinspection BusyWait
            Thread.sleep(SLEEP_PERIOD);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        } else {
          break;
        }
      }
    }

    private void minusReplicaDeltaFromRunning(int replicaDelta) {
      if (replicaDelta > 0) {
        runningReplications.addAndGet(-replicaDelta);
      } else {
        runningDeletions.addAndGet(replicaDelta);
      }
    }

    /**
     * @return null if not applicable
     */
    private HdfsLocatedFileStatus getFileStatus(FileState fileState) {
      if (fileState == null) {
        return null;
      }

      LOG.info("Checking file: " + fileState + " ...");
      try {
        if (!dfs.exists(fileState.filePath)) {
          LOG.info("Skip not exist file: " + fileState.filePath);
          return null;
        }

        HdfsFileStatus[] statuses;
        if (fileState.fileId == HdfsConstants.INVALIDATE_INODE_ID) {
          statuses = dfs.listPaths(
              fileState.filePath, HdfsFileStatus.EMPTY_NAME, true).getPartialListing();
        } else {
          statuses = dfs.listPaths(fileState.filePath, fileState.fileId,
              HdfsFileStatus.EMPTY_NAME, true).getPartialListing();
        }
        if (statuses[0].isDir()) {
          LOG.info("Skip directory: " + fileState.filePath);
          return null;
        }
        Preconditions.checkArgument(statuses[0] instanceof HdfsLocatedFileStatus);
        return (HdfsLocatedFileStatus)statuses[0];
      } catch (IOException e) {
        LOG.warn("getFileInfo({}) encountered {}", fileState.filePath, e);
        return null;
      }
    }

    private boolean areAllBlocksHaveCorrectReplicas(FileState fileState) {
      Preconditions.checkNotNull(fileState.getFileStatus());
      HdfsLocatedFileStatus status = fileState.getFileStatus();
      if (status.getLen() == 0) {
        LOG.info("Skip empty file: " + fileState.getFilePath());
        return true;
      }

      LocatedBlocks locatedBlocks = status.getBlockLocations();
      Preconditions.checkNotNull(locatedBlocks, "locatedBlocks should not be null!");

      short factor = status.getReplication();
      for (LocatedBlock block: locatedBlocks.getLocatedBlocks()) {
        if (block.getLocations().length != factor) {
          return false;
        }
      }
      return true;
    }
  }

  /**
   * A structure used to track the state of a file.
   */
  static class FileState {

    private final String filePath;
    private final long fileId;
    private long lastCheckTime = 0L;
    private int leftCheckTimes;
    // the number of replicas to add for the file
    private final int replicaDelta;
    private HdfsLocatedFileStatus fileStatus;
    private ReplicationRule rule;

    FileState(String filePath, ReplicationRule rule,
        int checkTimes, int replicaDelta) {
      this(filePath, HdfsConstants.INVALIDATE_INODE_ID, rule, checkTimes, replicaDelta);
    }

    FileState(String filePath, long fileId, ReplicationRule rule,
        int checkTimes, int replicaDelta) {
      this.filePath = filePath;
      this.fileId = fileId;
      this.rule = rule;
      this.leftCheckTimes = checkTimes;
      this.replicaDelta = replicaDelta;
    }

    public String getFilePath() {
      return filePath;
    }

    public long getLastCheckTime() {
      return lastCheckTime;
    }

    public void setLastCheckTime(long lastCheckTime) {
      this.lastCheckTime = lastCheckTime;
    }

    public int getLeftCheckTimes() {
      return leftCheckTimes;
    }

    public void setLeftCheckTimes(int leftCheckTimes) {
      this.leftCheckTimes = leftCheckTimes;
    }

    public int getReplicaDelta() {
      return replicaDelta;
    }

    public HdfsLocatedFileStatus getFileStatus() {
      return fileStatus;
    }

    public void setFileStatus(HdfsLocatedFileStatus fileStatus) {
      this.fileStatus = fileStatus;
    }

    public ReplicationRule getRule() {
      return rule;
    }

    public long getFileId() { return fileId; }

    public void setRule(ReplicationRule rule) { this.rule = rule; }

    @Override
    public String toString() {
      return "FileState{" +
          "filePath='" + filePath + '\'' +
          ", lastCheckTime=" + lastCheckTime +
          ", leftCheckTimes=" + leftCheckTimes +
          ", replicaDelta=" + replicaDelta +
          ", fileStatus=" + fileStatus +
          ", rule=" + rule +
          '}';
    }
  }
}
