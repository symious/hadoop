/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RefreshHandler;
import org.apache.hadoop.ipc.RefreshResponse;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_BLOCK_RECOVERY_TRIGGER_TIME_THRESHOLD_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_BLOCK_RECOVERY_TRIGGER_TIME_THRESHOLD_KEY;

public class DataNodeBlockRecoveryTriggerHandler implements RefreshHandler {

  private static final Logger LOG = LoggerFactory.getLogger(
      DataNodeBlockRecoveryTriggerHandler.class);

  public static final String DATANODE_BLOCK_RECOVERY_TRIGGER = "DataNodeBlockRecoveryTrigger";

  private long defaultTimeThreshold = DFS_NAMENODE_BLOCK_RECOVERY_TRIGGER_TIME_THRESHOLD_DEFAULT;
  private long preTriggerTime;
  private String preTriggerHost;
  private final DatanodeManager dnManager;

  public DataNodeBlockRecoveryTriggerHandler(Configuration conf, DatanodeManager dnManager) {
    if (conf != null) {
      this.defaultTimeThreshold = conf.getLong(
          DFS_NAMENODE_BLOCK_RECOVERY_TRIGGER_TIME_THRESHOLD_KEY,
          DFS_NAMENODE_BLOCK_RECOVERY_TRIGGER_TIME_THRESHOLD_DEFAULT);
    }
    this.dnManager = dnManager;
  }

  @Override
  public synchronized RefreshResponse handleRefresh(String identifier, String[] args) {
    if (identifier.equals(DATANODE_BLOCK_RECOVERY_TRIGGER)) {
      if (args == null || args.length != 1) {
        return new RefreshResponse(-1, "Please set the args, such as <DataNode Ip>.");
      }
      if (Time.monotonicNow() - preTriggerTime < defaultTimeThreshold) {
        return new RefreshResponse(-1,
            "Trigger too frequent, allow to trigger once every 3 minutes."
                + " The previous triggered DN is " + this.preTriggerHost);
      }

      String dataNodeHost = args[0];
      String[] hostAndPorts = dataNodeHost.split(":");
      DatanodeDescriptor datanode = null;
      if (hostAndPorts.length == 1) {
        datanode = this.dnManager.getDatanodeByHost(hostAndPorts[0]);
      } else if (hostAndPorts.length == 2) {
        datanode = this.dnManager.getDatanodeByXferAddr(hostAndPorts[0],
            Integer.parseInt(hostAndPorts[1]));
      }
      if (datanode != null) {
        try {
          LOG.info("Trigger BlockRecovery for {} with host {}.", datanode, dataNodeHost);
          boolean triggerResult = this.dnManager.removeDatanode(datanode);
          if (triggerResult) {
            this.preTriggerTime = Time.monotonicNow();
            this.preTriggerHost = dataNodeHost;
            return new RefreshResponse(0,
                "Successfully triggered block recovery for " + dataNodeHost);
          }
        } catch (IOException e) {
          return new RefreshResponse(-1, e.getMessage());
        }
      } else {
        return new RefreshResponse(-1, "Cannot find datanode for " + dataNodeHost);
      }
    }
    return new RefreshResponse(-1, "Failed");
  }
}
