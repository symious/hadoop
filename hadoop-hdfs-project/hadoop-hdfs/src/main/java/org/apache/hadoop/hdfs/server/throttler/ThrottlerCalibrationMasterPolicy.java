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

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An interface for throttling total bandwidths across all datanodes
 */
public abstract class ThrottlerCalibrationMasterPolicy {
  protected static final Logger LOG =
      LoggerFactory.getLogger(ThrottlerCalibrationMasterPolicy.class);

  protected final Configuration conf;
  protected final FSNamesystem fsn;

  public ThrottlerCalibrationMasterPolicy(Configuration conf, FSNamesystem fsNamesystem) {
    this.conf = conf;
    this.fsn = fsNamesystem;
  }

  /**
   * Reads the total read/write/transfer throughput up to now by a DN, then suggests new bandwidths.
   * Implementations of this method should return non-negative values for all bandwidth suggestions.
   * If any of the 3 suggestions is less than 0, all 3 suggestions will be ignored by DNs.
   * NNs cannot disable DN-side throttlers! Zero values are ignored by DNs instead of disabling throttlers.
   *
   * @param nodeReg DN to calibrate bandwidths for
   * @param readBytesThrottled total read bytes throttled
   * @param writeBytesThrottled total write bytes throttled
   * @param transferBytesThrottled total transfer bytes throttled
   * @return an array of size 3 with new bandwidth suggestions in this order: read->write->transfer
   */
  public abstract long[] getNewBandwidths(DatanodeRegistration nodeReg, long readBytesThrottled,
      long writeBytesThrottled, long transferBytesThrottled);

  public static ThrottlerCalibrationMasterPolicy newThrottlerCalibrationPolicy(Configuration conf,
      FSNamesystem fsNamesystem) {
    Class<? extends ThrottlerCalibrationMasterPolicy> clazz =
        conf.getClass(DFSConfigKeys.DFS_THROTTLER_CALIBRATION_MASTER_POLICY_CLASSNAME_KEY,
            DFSConfigKeys.DFS_THROTTLER_CALIBRATION_MASTER_POLICY_CLASSNAME_DEFAULT,
            ThrottlerCalibrationMasterPolicy.class);
    try {
      Constructor<?> constructor = clazz.getConstructor(Configuration.class, FSNamesystem.class);
      return (ThrottlerCalibrationMasterPolicy) constructor.newInstance(conf, fsNamesystem);
    } catch (ReflectiveOperationException e) {
      LOG.error("Could not instantiate: {}", clazz.getSimpleName(), e);
      return null;
    }
  }

  @VisibleForTesting
  public void setTimer(Timer timer) {
  }

  public void shutdown() {
    // Do nothing for the basic policy, different policies might want to do something
  }
}
