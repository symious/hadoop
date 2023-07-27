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
package org.apache.hadoop.hdfs.server.zoneservice.metrics;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZoneProgressTracker {
  private static final Logger LOG = LoggerFactory.getLogger(ZoneProgressTracker.class);

  public final static int UNTRACKED_DUMMY = -1;
  private static final AtomicInteger fileCount = new AtomicInteger();
  private static final AtomicLong byteCount = new AtomicLong();
  private static final AtomicLong blockCount = new AtomicLong();
  private static final ConcurrentMap<String, AtomicInteger> dispatches = new ConcurrentHashMap<>();
  private static int totalFiles = UNTRACKED_DUMMY;
  private static long start = -1;
  private static long lastByteLogged = -1;
  private static long lastFileLogged = -1;
  private static long lastBlockLogged = -1;

  private static long lastTrackerPrint = -1;
  private static long printPeriod = DFSConfigKeys.DFS_ZONE_PROGRESS_TRACKER_PRINT_PERIOD_DEFAULT;
  private static long filesPerPrint = DFSConfigKeys.DFS_ZONE_PROGRESS_TRACKER_FILES_PER_PRINT_DEFAULT;
  private static boolean doEstimateCompletionTime =
      DFSConfigKeys.DFS_ZONE_PROGRESS_TRACKER_ESTIMATE_COMPLETION_TIME_DEFAULT;
  private static ZoneProgressPrintModes printMode =
      DFSConfigKeys.DFS_ZONE_PROGRESS_TRACKER_PRINT_MODE_DEFAULT;
  private static Timer timer = new Timer();

  public static void initConf(Configuration conf) {
    printPeriod = conf.getLong(DFSConfigKeys.DFS_ZONE_PROGRESS_TRACKER_PRINT_PERIOD_KEY,
        DFSConfigKeys.DFS_ZONE_PROGRESS_TRACKER_PRINT_PERIOD_DEFAULT);
    filesPerPrint = conf.getInt(DFSConfigKeys.DFS_ZONE_PROGRESS_TRACKER_FILES_PER_PRINT_KEY,
        DFSConfigKeys.DFS_ZONE_PROGRESS_TRACKER_FILES_PER_PRINT_DEFAULT);
    doEstimateCompletionTime =
        conf.getBoolean(DFSConfigKeys.DFS_ZONE_PROGRESS_TRACKER_ESTIMATE_COMPLETION_TIME_KEY,
            DFSConfigKeys.DFS_ZONE_PROGRESS_TRACKER_ESTIMATE_COMPLETION_TIME_DEFAULT);
    printMode = conf.getEnum(DFSConfigKeys.DFS_ZONE_PROGRESS_TRACKER_PRINT_MODE_KEY,
        DFSConfigKeys.DFS_ZONE_PROGRESS_TRACKER_PRINT_MODE_DEFAULT);
  }

  @VisibleForTesting
  public static void setTimer(Timer newTimer) {
    timer = newTimer;
  }

  public enum ZoneProgressPrintModes {
    EVERY_N_FILES,
    PERIODICALLY
  }

  @VisibleForTesting
  public synchronized static void resetTracker() {
    fileCount.set(0);
    byteCount.set(0);
    blockCount.set(0);
    dispatches.clear();
    totalFiles = UNTRACKED_DUMMY;
    start = -1;
    lastByteLogged = -1;
    lastFileLogged = -1;
  }

  /**
   * Prints the current progress of a zoneservice procedure.
   */
  public static void printProgress() {
    int fileCountSnapshot = fileCount.get();

    if (printMode == ZoneProgressPrintModes.PERIODICALLY) {
      long now = timer.monotonicNow();
      if (now - lastTrackerPrint > printPeriod) {
        lastTrackerPrint = now;
      } else {
        return;
      }
    } else if (printMode == ZoneProgressPrintModes.EVERY_N_FILES) {
      if (fileCountSnapshot % filesPerPrint != 0) {
        return ;
      }
    }

    double elapsedForFile = lastFileLogged - start;
    double elapsedForByte = lastByteLogged - start;
    double elapsedForBlock = lastBlockLogged - start;
    long byteCountSnapshot = byteCount.get();
    long blockCountSnapshot = blockCount.get();

    String msg = "Zoneservice progress\n"
        + "Elapsed time: %f ms\n"
        + "Files: %d/%d (%5.2f%%), rate: %f files/s\n"
        + "Blocks: %d, rate: %f blocks/s\n"
        + "Bytes: %d, rate: %f bytes/s\n"
        + "ETC: %fs.\n";
    double filesRate = fileCountSnapshot / elapsedForFile * 1000;
    double blocksRate = blockCountSnapshot / elapsedForByte * 1000;
    double bytesRate = byteCountSnapshot / elapsedForBlock * 1000;
    double estimatedTimeToComplete =
        totalFiles == UNTRACKED_DUMMY ? -1 : (totalFiles - fileCountSnapshot) / filesRate;
    LOG.info(String.format(msg,
        elapsedForFile,
        fileCountSnapshot, totalFiles, (double) 100 * fileCountSnapshot / totalFiles, filesRate,
        blockCountSnapshot, blocksRate,
        byteCountSnapshot, bytesRate,
        estimatedTimeToComplete));
  }

  public static void incrFileCount() {
    fileCount.addAndGet(1);
    lastFileLogged = timer.monotonicNow();
    printProgress();
  }

  public static void addByteCount(long bytesDone) {
    byteCount.addAndGet(bytesDone);
    lastByteLogged = timer.monotonicNow();
  }

  public static void incrBlockCount() {
    blockCount.incrementAndGet();
    lastBlockLogged = timer.monotonicNow();
  }

  public static void queueFile(String path) {
    if (!dispatches.containsKey(path)) {
      synchronized (dispatches) {
        if (!dispatches.containsKey(path)) {
          dispatches.put(path, new AtomicInteger());
        }
      }
    }
    dispatches.get(path).incrementAndGet();
  }

  public static void dequeueFile(String path) {
    assert dispatches.containsKey(path) : "Trying to dequeue a nonexistent path " + path;
    if (dispatches.get(path).decrementAndGet() == 0) {
      synchronized (dispatches) {
        dispatches.remove(path);
      }
      incrFileCount();
    }
  }

  /**
   * One off call to get the total number of files to estimate progress.
   * Can be expensive despite being a one time thing due to the recursive traversal,
   * consider disabling for big paths.
   * @param fs filesystem object
   * @param targetPaths paths to track
   */
  public synchronized static void trackPaths(FileSystem fs, List<Path> targetPaths) {
    start = timer.monotonicNow();
    if (!doEstimateCompletionTime) {
      return;
    }

    totalFiles = 0;
    for (Path targetPath : targetPaths) {
      try {
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(targetPath, true);
        while (files.hasNext()) {
          totalFiles++;
          files.next();
        }
      } catch (FileNotFoundException fnfe) {
        // Just ignore non existent paths
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
    if (totalFiles <= 0) {
      totalFiles = UNTRACKED_DUMMY;
    }
  }

  public static void checkForLeak() {
    if (!dispatches.isEmpty()) {
      LOG.warn("ZoneProgressTracker leak detected.");
      for (Map.Entry<String, AtomicInteger> entry : dispatches.entrySet()) {
        LOG.warn("path={},counter={}", entry.getKey(), entry.getValue().get());
      }
    }
  }

  @VisibleForTesting
  public static int getTrackedTotalFiles() {
    return totalFiles;
  }

  @VisibleForTesting
  public static int getFileCount() {
    return fileCount.get();
  }

  @VisibleForTesting
  public static long getBlockCount() {
    return blockCount.get();
  }

  @VisibleForTesting
  public static long getByteCount() {
    return byteCount.get();
  }
}
