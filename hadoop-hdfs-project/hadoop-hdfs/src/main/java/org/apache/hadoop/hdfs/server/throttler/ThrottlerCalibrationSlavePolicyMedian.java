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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.util.Timer;

/**
 * A simple manager that applies the median of suggestions from all namenodes
 */
public class ThrottlerCalibrationSlavePolicyMedian extends ThrottlerCalibrationSlavePolicy {
  private final long dnThrottlerCalibrateInterval;
  private Timer timer;
  private long lastCalibration = 0;
  private final ConcurrentMap<InetSocketAddress, Long> suggestedReadBandwidths =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<InetSocketAddress, Long> suggestedWriteBandwidths =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<InetSocketAddress, Long> suggestedTransferBandwidths =
      new ConcurrentHashMap<>();

  public ThrottlerCalibrationSlavePolicyMedian(Configuration conf) {
    super(conf);
    this.timer = new Timer();
    this.dnThrottlerCalibrateInterval = conf.getTimeDuration(
        DFSConfigKeys.DFS_THROTTLER_FIRST_COME_FIRST_SERVED_POLICY_CALIBRATION_INTERVAL_KEY,
        DFSConfigKeys.DFS_THROTTLER_FIRST_COME_FIRST_SERVED_POLICY_CALIBRATION_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS);
  }

  @Override
  public void setTimer(Timer timer) {
    this.timer = timer;
  }

  @Override
  public long[] getNewBandwidths(InetSocketAddress nnAddr, long readBytesThrottled,
      long writeBytesThrottled, long transferBytesThrottled) {
    suggestedReadBandwidths.put(nnAddr, readBytesThrottled);
    suggestedWriteBandwidths.put(nnAddr, writeBytesThrottled);
    suggestedTransferBandwidths.put(nnAddr, transferBytesThrottled);
    if (timer.monotonicNow() - lastCalibration > dnThrottlerCalibrateInterval) {
      lastCalibration = timer.monotonicNow();
      return new long[] { getMedian(suggestedReadBandwidths.values()),
          getMedian(suggestedWriteBandwidths.values()),
          getMedian(suggestedTransferBandwidths.values()) };
    } else {
      return new long[] { 0, 0, 0 };
    }
  }

  private long getMedian(Collection<Long> collection) {
    if (collection.isEmpty()) {
      return 0;
    }
    if (collection.size() == 1) {
      return new ArrayList<>(collection).get(0);
    }
    List<Long> array = new ArrayList<>(collection);
    Collections.sort(array);
    long median = array.get(array.size() / 2);
    if (array.size() % 2 == 0) {
      median += array.get((array.size() / 2) - 1);
      median /= 2;
    }
    return median;
  }
}
