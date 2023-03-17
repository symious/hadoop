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
package org.apache.hadoop.hdfs.server.throttler;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.Sets;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.util.concurrent.HadoopExecutors;

/**
 * This throttler calibration policy applies a minimum bandwidth for inactive (traffic = 0) DNs
 * and equally distributes the bandwidths over the other live DNs
 */
public class ThrottlerCalibrationMasterPolicyAverageActiveNodes
    extends ThrottlerCalibrationMasterPolicy {

  private long calibrateInterval;
  private final boolean isReadThrottlerCalibrationEnabled;
  private final boolean isWriteThrottlerCalibrationEnabled;
  private final boolean isTransferThrottlerCalibrationEnabled;
  private final long masterReadBandwidth;
  private final long masterWriteBandwidth;
  private final long masterTransferBandwidth;
  private final int maxFactor;
  private final long minBandwidth;

  private final CalibrationService calibrationService;
  private final ConcurrentMap<String, Long> lastReadBytes = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Long> lastWriteBytes = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Long> lastTransferBytes = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Long> currentReadBytes = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Long> currentWriteBytes = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Long> currentTransferBytes = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Long> cachedReadBandwidthSuggestions =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Long> cachedWriteBandwidthSuggestions =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Long> cachedTransferBandwidthSuggestions =
      new ConcurrentHashMap<>();
  private final long activeThreshold;
  private long gracePeriod;

  private static final ScheduledExecutorService SCHEDULED_EXECUTOR =
      HadoopExecutors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setDaemon(true)
          .setNameFormat("ThrottlerCalibrationMasterPolicyAverageActiveNodes").build());
  private final ScheduledFuture<?> calibrationTask; // Task to calibrate bandwidths in background

  public ThrottlerCalibrationMasterPolicyAverageActiveNodes(Configuration conf, FSNamesystem fsn) {
    super(conf, fsn);
    this.calibrateInterval = conf.getTimeDuration(
        DFSConfigKeys.DFS_THROTTLER_AVERAGE_ACTIVE_POLICY_CALIBRATION_INTERVAL_KEY,
        DFSConfigKeys.DFS_THROTTLER_AVERAGE_ACTIVE_POLICY_CALIBRATION_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS);
    this.gracePeriod =
        conf.getTimeDuration(DFSConfigKeys.DFS_THROTTLER_AVERAGE_ACTIVE_POLICY_GRACE_PERIOD_KEY,
            DFSConfigKeys.DFS_THROTTLER_AVERAGE_ACTIVE_POLICY_GRACE_PERIOD_DEFAULT,
            TimeUnit.MILLISECONDS);
    if (this.calibrateInterval <= 0) {
      LOG.info("Invalid calibration interval {}, using default value {}", this.calibrateInterval,
          DFSConfigKeys.DFS_THROTTLER_AVERAGE_ACTIVE_POLICY_CALIBRATION_INTERVAL_DEFAULT);
      this.calibrateInterval =
          DFSConfigKeys.DFS_THROTTLER_AVERAGE_ACTIVE_POLICY_CALIBRATION_INTERVAL_DEFAULT;
    }
    if (this.gracePeriod <= 0) {
      LOG.info("Invalid grace period {}, using default value {}", this.gracePeriod,
          DFSConfigKeys.DFS_THROTTLER_AVERAGE_ACTIVE_POLICY_GRACE_PERIOD_DEFAULT);
      this.gracePeriod = DFSConfigKeys.DFS_THROTTLER_AVERAGE_ACTIVE_POLICY_GRACE_PERIOD_DEFAULT;
    }

    this.masterReadBandwidth =
        conf.getLongBytes(DFSConfigKeys.DFS_THROTTLER_READ_MASTER_BANDWIDTH_KEY,
            DFSConfigKeys.DFS_THROTTLER_READ_MASTER_BANDWIDTH_DEFAULT);
    this.masterWriteBandwidth =
        conf.getLongBytes(DFSConfigKeys.DFS_THROTTLER_WRITE_MASTER_BANDWIDTH_KEY,
            DFSConfigKeys.DFS_THROTTLER_WRITE_MASTER_BANDWIDTH_DEFAULT);
    this.masterTransferBandwidth =
        conf.getLongBytes(DFSConfigKeys.DFS_THROTTLER_TRANSFER_MASTER_BANDWIDTH_KEY,
            DFSConfigKeys.DFS_THROTTLER_TRANSFER_MASTER_BANDWIDTH_DEFAULT);

    this.isReadThrottlerCalibrationEnabled = this.masterReadBandwidth > 0;
    this.isWriteThrottlerCalibrationEnabled = this.masterWriteBandwidth > 0;
    this.isTransferThrottlerCalibrationEnabled = this.masterTransferBandwidth > 0;

    this.activeThreshold =
        conf.getLongBytes(DFSConfigKeys.DFS_THROTTLER_AVERAGE_ACTIVE_POLICY_ACTIVE_THRESHOLD_KEY,
            DFSConfigKeys.DFS_THROTTLER_AVERAGE_ACTIVE_POLICY_ACTIVE_THRESHOLD_DEFAULT);
    this.maxFactor = conf.getInt(DFSConfigKeys.DFS_THROTTLER_AVERAGE_ACTIVE_POLICY_MAX_FACTOR_KEY,
        DFSConfigKeys.DFS_THROTTLER_AVERAGE_ACTIVE_POLICY_MAX_FACTOR_DEFAULT);
    this.minBandwidth = conf.getLongBytes(
        DFSConfigKeys.DFS_THROTTLER_AVERAGE_ACTIVE_POLICY_MINIMUM_SLAVE_BANDWIDTH_KEY,
        DFSConfigKeys.DFS_THROTTLER_AVERAGE_ACTIVE_POLICY_MINIMUM_SLAVE_BANDWIDTH_DEFAULT);

    calibrationService = new CalibrationService();
    calibrationTask =
        SCHEDULED_EXECUTOR.scheduleWithFixedDelay(calibrationService, this.gracePeriod,
            this.calibrateInterval, TimeUnit.SECONDS);
  }

  @Override
  public void shutdown() {
    if (calibrationTask != null) {
      calibrationTask.cancel(true);
    }
  }

  @VisibleForTesting
  public void triggerCalibrationForTesting() {
    this.calibrationService.run();
  }

  class CalibrationService implements Runnable {

    @Override
    public synchronized void run() {
      purgeDeadDNs();

      cacheNewBandwidthSuggestions(isReadThrottlerCalibrationEnabled, currentReadBytes,
          lastReadBytes, cachedReadBandwidthSuggestions, masterReadBandwidth);
      cacheNewBandwidthSuggestions(isWriteThrottlerCalibrationEnabled, currentWriteBytes,
          lastWriteBytes, cachedWriteBandwidthSuggestions, masterWriteBandwidth);
      cacheNewBandwidthSuggestions(isTransferThrottlerCalibrationEnabled, currentTransferBytes,
          lastTransferBytes, cachedTransferBandwidthSuggestions, masterTransferBandwidth);
    }

    /**
     * Sync with FSNamesystem once every cycle to remove dead DNs
     */
    private void purgeDeadDNs() {
      if (fsn == null) {
        return;
      }

      List<DatanodeDescriptor> liveNodes = fsn.getBlockManager().getDatanodeManager()
          .getDatanodeListForReport(HdfsConstants.DatanodeReportType.LIVE);
      Set<String> liveUUIDs = new HashSet<>();
      for (DatanodeDescriptor liveNode : liveNodes) {
        liveUUIDs.add(liveNode.getDatanodeUuid());
      }

      for (String key : Sets.difference(currentReadBytes.keySet(), liveUUIDs)) {
        currentReadBytes.remove(key);
      }
      for (String key : Sets.difference(currentWriteBytes.keySet(), liveUUIDs)) {
        currentWriteBytes.remove(key);
      }
      for (String key : Sets.difference(currentTransferBytes.keySet(), liveUUIDs)) {
        currentTransferBytes.remove(key);
      }
    }

    private void cacheNewBandwidthSuggestions(boolean enableFlag,
        ConcurrentMap<String, Long> currentBytesDict, ConcurrentMap<String, Long> lastBytesDict,
        ConcurrentMap<String, Long> cachedSuggestionsDict, long masterBandwidth) {
      if (enableFlag) {
        Set<String> activeNodes = new HashSet<>();
        Set<String> inactiveNodes = new HashSet<>();
        for (String uuid : currentBytesDict.keySet()) {
          long lastBytesThrottled = lastBytesDict.containsKey(uuid) ? lastBytesDict.get(uuid) : 0;
          long currentBytesThrottled = currentBytesDict.get(uuid);
          long changeSinceLast = currentBytesThrottled - lastBytesThrottled;
          if (changeSinceLast > activeThreshold) {
            activeNodes.add(uuid);
          } else {
            inactiveNodes.add(uuid);
          }
          lastBytesDict.put(uuid, currentBytesThrottled);
        }
        for (String uuid : inactiveNodes) {
          cachedSuggestionsDict.put(uuid, minBandwidth);
        }
        if (!activeNodes.isEmpty()) {
          long activeBandwidth = Math.max(getNewBandwidth(masterBandwidth, activeNodes.size(), lastBytesDict.size()),
              minBandwidth);
          for (String uuid : activeNodes) {
            cachedSuggestionsDict.put(uuid, activeBandwidth);
          }
        }
        currentBytesDict.clear();
      }
    }

    private long getNewBandwidth(long masterBandwidth, int activeNodes, int totalNodes) {
      long maxThroughput = masterBandwidth / totalNodes * Math.min(totalNodes, maxFactor);
      if (activeNodes == 0) {
        return maxThroughput;
      }
      long averageThroughput = masterBandwidth / activeNodes;
      return Math.min(averageThroughput, maxThroughput);
    }
  }

  /**
   * Just grab the bandwidths directly from cache dicts. Calibration magic is done by the daemon.
   */
  @Override
  public long[] getNewBandwidths(DatanodeRegistration nodeReg, long readBytesThrottled,
      long writeBytesThrottled, long transferBytesThrottled) {
    String uuid = nodeReg.getDatanodeUuid();
    // Plug bytes throttled into calibration daemon
    currentReadBytes.put(uuid, readBytesThrottled);
    currentWriteBytes.put(uuid, writeBytesThrottled);
    currentTransferBytes.put(uuid, transferBytesThrottled);

    // Read bandwidth suggestions from calibration daemon
    long readBW = 0;
    long writeBW = 0;
    long transferBW = 0;

    if (cachedReadBandwidthSuggestions.containsKey(uuid)) {
      readBW = cachedReadBandwidthSuggestions.get(uuid);
    }
    if (cachedWriteBandwidthSuggestions.containsKey(uuid)) {
      writeBW = cachedWriteBandwidthSuggestions.get(uuid);
    }
    if (cachedTransferBandwidthSuggestions.containsKey(uuid)) {
      transferBW = cachedTransferBandwidthSuggestions.get(uuid);
    }

    return new long[] { readBW, writeBW, transferBW };
  }
}
