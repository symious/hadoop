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
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.util.Timer;

/**
 * A simple manager that applies the first set of valid bandwidths it receives
 */
public class ThrottlerCalibrationSlavePolicyFirstComeFirstServed
    extends ThrottlerCalibrationSlavePolicy {
  private final long dnThrottlerCalibrateInterval;
  private Timer timer;
  private long lastCalibration = 0;

  public ThrottlerCalibrationSlavePolicyFirstComeFirstServed(Configuration conf) {
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

  /**
   * Return the first set of bandwidths received once the refresh interval passes.
   * Just ignore the nn address.
   */
  @Override
  public long[] getNewBandwidths(InetSocketAddress nnAddr, long readBytesThrottled, long writeBytesThrottled,
      long transferBytesThrottled) {
    if (timer.monotonicNow() - lastCalibration > dnThrottlerCalibrateInterval) {
      lastCalibration = timer.monotonicNow();
      return new long[] { readBytesThrottled, writeBytesThrottled, transferBytesThrottled };
    } else {
      return new long[] { 0, 0, 0 };
    }
  }
}
