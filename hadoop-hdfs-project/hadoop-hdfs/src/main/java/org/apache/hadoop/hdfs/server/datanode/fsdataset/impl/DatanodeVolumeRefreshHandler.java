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
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.ipc.RefreshHandler;
import org.apache.hadoop.ipc.RefreshResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DatanodeVolumeRefreshHandler implements RefreshHandler {

  private static final Logger LOG = LoggerFactory.getLogger(DatanodeVolumeRefreshHandler.class);

  public static final String DATANODE_VOLUME_REFRESH_HANDLER_IDENTIFIER =
      "RefreshDatanodeVolumeConfigs";
  private final DataNode dataNode;

  public DatanodeVolumeRefreshHandler(DataNode datanode) {
    this.dataNode = datanode;
  }

  @Override
  public RefreshResponse handleRefresh(String identifier, String[] args) {
    if (identifier.equals(DATANODE_VOLUME_REFRESH_HANDLER_IDENTIFIER)) {
      if (args == null || args.length != 2) {
        return new RefreshResponse(-1,
            "Please set the args, such as <addVolume|removeVolume|failVolume> <volume location>.");
      }
      String operationType = args[0];
      String volumeLocation = args[1];
      try {
        LOG.info("Dynamic volume refresh requested: op={} volume={}", operationType, volumeLocation);
        switch (operationType) {
          case "addVolume" : {
            dataNode.addVolume(volumeLocation);
            return RefreshResponse.successResponse();
          }
          case "removeVolume" : {
            dataNode.removeVolume(volumeLocation);
            return RefreshResponse.successResponse();
          }
          case "failVolume" : {
            dataNode.handleFailureVolume(volumeLocation);
            return RefreshResponse.successResponse();
          }
          default: {
            // do nothing.
          }
        }
      } catch (IOException e) {
        return new RefreshResponse(-1, e.getMessage());
      }
    }
    return new RefreshResponse(-1, "Failed");
  }
}
