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
package org.apache.hadoop.hdfs.server.namenode.handler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.ipc.RefreshHandler;
import org.apache.hadoop.ipc.RefreshResponse;

public class DatanodeManagerRefreshHandler implements RefreshHandler {

  public static final String DATANODE_MANAGER_REFRESH_HANDLER_IDENTIFIER =
      "RefreshDatanodeManagerConfigs";
  private final DatanodeManager datanodeManager;

  public DatanodeManagerRefreshHandler(DatanodeManager datanodeManager) {
    this.datanodeManager = datanodeManager;
  }

  @Override
  public RefreshResponse handleRefresh(String identifier, String[] args) {
    if (identifier.equals(DATANODE_MANAGER_REFRESH_HANDLER_IDENTIFIER)) {
      Configuration conf = new HdfsConfiguration();
      if (args == null || args.length == 0) {
        return new RefreshResponse(-1, "Please set the args, such as dnsToSwitchMapping.");
      }
      String operationType = args[0];
      switch (operationType) {
        case "dnsToSwitchMapping" : {
          datanodeManager.reloadDNSToSwitchMapping(conf);
          return RefreshResponse.successResponse();
        }
        default: {
          // do nothing.
        }
      }
    }
    return new RefreshResponse(-1, "Failed");
  }
}
