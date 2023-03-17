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


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;

/**
 * This throttler calibration policy doesn't suggest any bandwidth for datanodes
 */
public class ThrottlerCalibrationMasterPolicyNoCalibration
    extends ThrottlerCalibrationMasterPolicy {

  public ThrottlerCalibrationMasterPolicyNoCalibration(Configuration conf,
      FSNamesystem fsNamesystem) {
    super(conf, fsNamesystem);
  }

  @Override
  public long[] getNewBandwidths(DatanodeRegistration nodeReg, long readBytesThrottled,
      long writeBytesThrottled, long transferBytesThrottled) {
    return new long[] { 0, 0, 0 };
  }
}
