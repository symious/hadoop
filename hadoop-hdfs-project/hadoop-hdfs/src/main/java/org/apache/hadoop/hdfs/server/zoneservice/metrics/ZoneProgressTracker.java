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
  private static Thread totalFilesTrackerThread = null;
  private static long start = -1;
  private static long lastByteLogged = -1;
  private static long lastFileLogged = -1;
  private static long lastBlockLogged = -1;

  private static long lastTrackerPrint = -1;
  private static long printPeriod = DFSConfigKeys.DFS_ZONE_PROGRESS_TRACKER_PRINT_PERIOD_DEFAULT;
  private static long filesPerPrint =
      DFSConfigKeys.DFS_ZONE_PROGRESS_TRACKER_FILES_PER_PRINT_DEFAULT;
  private static boolean doEstimateCompletionTime =
      DFSConfigKeys.DFS_ZONE_PROGRESS_TRACKER_ESTIMATE_COMPLETION_TIME_DEFAULT;
  private static ZoneProgressPrintModes printMode =
      DFSConfigKeys.DFS_ZONE_PROGRESS_TRACKER_PRINT_MODE_DEFAULT;
  private static Timer timer = new Timer();
  // Various debug timestamps below
  private static long initTimeStart;
  private static long processPathStartTime;
  private static long coordinatorWaitStartTime;
  private static long fetcherWaitStartTime;
  private static long moveCompletionWaitStartTime;
  private static long postProcessingStartTime;

  private static int fileCountLastPrint;
  private static long byteCountLastPrint;
  private static long blockCountLastPrint;
  private static final ArithmeticMeanRoller setReplicationRoller = new ArithmeticMeanRoller();
  private static final ArithmeticMeanRoller coordinatorSleepRoller = new ArithmeticMeanRoller();
  private static final ArithmeticMeanRoller waitFilesQueueRoller = new ArithmeticMeanRoller();
  private static final ArithmeticMeanRoller finishFilesQueueRoller = new ArithmeticMeanRoller();

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

  public synchronized static void startCountingInitTime() {
    initTimeStart = timer.monotonicNow();
  }

  public synchronized static void startCountingProcessPathTime() {
    processPathStartTime = timer.monotonicNow();
  }

  public synchronized static void startCountingCoordinatorWaitTime() {
    coordinatorWaitStartTime = timer.monotonicNow();
  }

  public synchronized static void startCountingFetcherWaitTime() {
    fetcherWaitStartTime = timer.monotonicNow();
  }

  public synchronized static void startCountingMoveCompletionWaitTime() {
    moveCompletionWaitStartTime = timer.monotonicNow();
  }

  public synchronized static void startCountingPostProcessingTime() {
    postProcessingStartTime = timer.monotonicNow();
  }

  public synchronized static void finishCountingInitTimeAndLog() {
    LOG.debug("ZoneMover initialization time: {}ms", timer.monotonicNow() - initTimeStart);
  }

  public synchronized static void finishCountingProcessPathTimeAndLog() {
    LOG.debug(
        "Time to initialize all dispatchers: {}ms, of which setReplication took {}ms, coordinator slept {}ms",
        timer.monotonicNow() - processPathStartTime, setReplicationRoller.sum,
        coordinatorSleepRoller.sum);
  }

  public synchronized static void finishCountingCoordinatorWaitTimeAndLog() {
    LOG.debug("ZoneReplicationCoordinator finished: {}ms",
        timer.monotonicNow() - coordinatorWaitStartTime);
  }

  public synchronized static void finishCountingFetcherWaitTimeAndLog() {
    LOG.debug("ZoneMover.Fetcher finished: {}ms", timer.monotonicNow() - fetcherWaitStartTime);
    LOG.info("Average setReplication time: {}ms", setReplicationRoller.getTruncatedMean());
    LOG.info("Average coordinator sleep time: {}ms", coordinatorSleepRoller.getTruncatedMean());
    LOG.info("Average time spent in waitFiles: {}ms", waitFilesQueueRoller.getTruncatedMean());
    LOG.info("Average time spent in finishFiles: {}ms", finishFilesQueueRoller.getTruncatedMean());
  }

  public synchronized static void finishCountingMoveCompletionWaitTimeAndLog() {
    LOG.debug("All moves finished in {}ms", timer.monotonicNow() - moveCompletionWaitStartTime);
  }

  public synchronized static void finishCountingPostProcessingTimeAndLog() {
    LOG.debug("Post processing finished in {}ms", timer.monotonicNow() - postProcessingStartTime);
  }

  // No need for any kind of thread-safe for the methods below, only used by a blocking main thread
  public static void addSetReplicationTime(long l) {
    setReplicationRoller.addNumber(l);
  }

  public static void addCoordinatorSleepTime(long l) {
    coordinatorSleepRoller.addNumber(l);
  }

  public static void addTimeSpentInWaitFilesQueue(long l) {
    waitFilesQueueRoller.addNumber(l);
  }

  public static void addTimeSpentInFinishFilesQueue(long l) {
    finishFilesQueueRoller.addNumber(l);
  }

  public enum ZoneProgressPrintModes {
    EVERY_N_FILES, PERIODICALLY
  }

  public synchronized static void resetTracker() {
    if (totalFilesTrackerThread != null) {
      totalFilesTrackerThread.interrupt();
      totalFilesTrackerThread = null;
    }
    fileCount.set(0);
    byteCount.set(0);
    blockCount.set(0);
    dispatches.clear();
    totalFiles = UNTRACKED_DUMMY;
    start = -1;
    lastByteLogged = -1;
    lastFileLogged = -1;
    processPathStartTime = 0;
    coordinatorWaitStartTime = 0;
    fetcherWaitStartTime = 0;
    moveCompletionWaitStartTime = 0;
    postProcessingStartTime = 0;
    setReplicationRoller.reset();
    coordinatorSleepRoller.reset();
    waitFilesQueueRoller.reset();
    finishFilesQueueRoller.reset();
  }

  /**
   * Prints the current progress of a zoneservice procedure.
   */
  public static void printProgress() {
    int fileCountSnapshot = fileCount.get();
    long timeSinceLastPrint = -1;

    long now = timer.monotonicNow();
    if (printMode == ZoneProgressPrintModes.PERIODICALLY) {
      if (now - lastTrackerPrint > printPeriod) {
        timeSinceLastPrint = now - lastTrackerPrint;
        lastTrackerPrint = now;
      } else {
        return;
      }
    } else if (printMode == ZoneProgressPrintModes.EVERY_N_FILES) {
      if (fileCountSnapshot % filesPerPrint != 0) {
        return ;
      } else {
        timeSinceLastPrint = now - lastTrackerPrint;
        lastTrackerPrint = now;
      }
    }

    double elapsedForFile = lastFileLogged - start;
    double elapsedForByte = lastByteLogged - start;
    double elapsedForBlock = lastBlockLogged - start;
    long byteCountSnapshot = byteCount.get();
    long blockCountSnapshot = blockCount.get();
    int fileCountSinceLastPrint = fileCountSnapshot - fileCountLastPrint;
    long byteCountSinceLastPrint = byteCountSnapshot - byteCountLastPrint;
    long blockCountSinceLastPrint = blockCountSnapshot - blockCountLastPrint;

    String msg = "Zoneservice progress\n"
        + "Elapsed time: %f ms; Since last report: %d ms\n"
        + "Files: %d/%d (%5.2f%%), rate: %f files/s\n"
        + "Blocks: %d, rate: %f blocks/s\n"
        + "Bytes: %d, rate: %f bytes/s\n"
        + "Files since last report: %d, rate: %f files/s\n"
        + "Blocks since last report: %d, rate: %f blocks/s\n"
        + "Bytes since last report: %d, rate: %f bytes/s\n"
        + "ETC: %fs\n"
        + "Average time spent in waitFiles: %d ms\n"
        + "Average time spent in finishFiles: %d ms\n"
        + "Average setReplication time: %d ms\n"
        + "Average coordinator sleep time: %d ms.\n";
    double filesRate = fileCountSnapshot / elapsedForFile * 1000;
    double blocksRate = blockCountSnapshot / elapsedForByte * 1000;
    double bytesRate = byteCountSnapshot / elapsedForBlock * 1000;
    double estimatedTimeToComplete =
        totalFiles == UNTRACKED_DUMMY ? -1 : (totalFiles - fileCountSnapshot) / filesRate;
    LOG.info(String.format(msg,
        elapsedForFile, timeSinceLastPrint,
        fileCountSnapshot, totalFiles, (double) 100 * fileCountSnapshot / totalFiles, filesRate,
        blockCountSnapshot, blocksRate,
        byteCountSnapshot, bytesRate,
        fileCountSinceLastPrint, (double) fileCountSinceLastPrint / timeSinceLastPrint * 1000,
        blockCountSinceLastPrint, (double) blockCountSinceLastPrint / timeSinceLastPrint * 1000,
        byteCountSinceLastPrint, (double) byteCountSinceLastPrint / timeSinceLastPrint * 1000,
        estimatedTimeToComplete,
        waitFilesQueueRoller.getTruncatedMean(),
        finishFilesQueueRoller.getTruncatedMean(),
        setReplicationRoller.getTruncatedMean(),
        coordinatorSleepRoller.getTruncatedMean()));
    fileCountLastPrint = fileCountSnapshot;
    byteCountLastPrint = byteCountSnapshot;
    blockCountLastPrint = blockCountSnapshot;
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
  public synchronized static void trackPaths(final FileSystem fs, final List<Path> targetPaths) {
    start = timer.monotonicNow();
    if (!doEstimateCompletionTime) {
      return;
    }
    assert totalFilesTrackerThread == null;

    totalFilesTrackerThread = new Thread(new Runnable() {
      @Override
      public void run() {
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
    }, "ZoneProgressTracker-TotalFileCounter");
    totalFilesTrackerThread.start();
  }

  @VisibleForTesting
  public static void waitForTotalFilesTrackerToFinish() throws InterruptedException {
    if (totalFilesTrackerThread != null) {
      totalFilesTrackerThread.join();
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

  static class ArithmeticMeanRoller {
    long count = 0;
    long sum = 0;

    public void reset() {
      count = 0;
      sum = 0;
    }

    public void addNumber(long num) {
      count++;
      sum += num;
    }

    /**
     * Usage is not thread-safe, but in general safe to do so since thread-safety is not critical.
     */
    public long getTruncatedMean() {
      if (count == 0) {
        return 0;
      }
      return sum / count;
    }
  }
}
