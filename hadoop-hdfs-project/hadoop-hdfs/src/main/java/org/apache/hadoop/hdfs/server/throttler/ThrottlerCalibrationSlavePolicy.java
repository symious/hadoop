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

import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An interface used by DN for refreshing throttlers based on suggested bandwidths by NNs
 */
public abstract class ThrottlerCalibrationSlavePolicy {
  protected static final Logger LOG =
      LoggerFactory.getLogger(ThrottlerCalibrationSlavePolicy.class);

  protected final Configuration conf;

  protected final AtomicLong readBytesThrottled = new AtomicLong(0);
  protected final AtomicLong writeBytesThrottled = new AtomicLong(0);
  protected final AtomicLong transferBytesThrottled = new AtomicLong(0);

  public ThrottlerCalibrationSlavePolicy(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Decides on a set of new bandwidths using suggested bandwidths by NNs
   * @return 3 new bandwidths in this order: [read, write, transfer]
   */
  public abstract long[] getNewBandwidths(InetSocketAddress nnAddr, long newReadBandwidth,
      long newWriteBandwidth, long newTransferBandwidth);

  public long getReadBytesThrottled() {
    return readBytesThrottled.get();
  }

  public long getWriteBytesThrottled() {
    return writeBytesThrottled.get();
  }

  public long getTransferBytesThrottled() {
    return transferBytesThrottled.get();
  }

  public AtomicLong getReadBytesThrottledAL() {
    return readBytesThrottled;
  }

  public AtomicLong getWriteBytesThrottledAL() {
    return writeBytesThrottled;
  }

  public AtomicLong getTransferBytesThrottledAL() {
    return transferBytesThrottled;
  }


  public static ThrottlerCalibrationSlavePolicy newThrottlerCalibrationSlavePolicy(
      Configuration conf) {
    Class<? extends ThrottlerCalibrationSlavePolicy> clazz =
        conf.getClass(DFSConfigKeys.DFS_THROTTLER_CALIBRATION_SLAVE_POLICY_CLASSNAME_KEY,
            DFSConfigKeys.DFS_THROTTLER_CALIBRATION_SLAVE_POLICY_CLASSNAME_DEFAULT,
            ThrottlerCalibrationSlavePolicy.class);
    try {
      Constructor<?> constructor = clazz.getConstructor(Configuration.class);
      return (ThrottlerCalibrationSlavePolicy) constructor.newInstance(conf);
    } catch (ReflectiveOperationException e) {
      LOG.error("Could not instantiate: {}", clazz.getSimpleName(), e);
      return null;
    }
  }

  @VisibleForTesting
  public void setTimer(Timer fakeTimer) {
  }
}
