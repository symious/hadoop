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
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.ipc.RefreshHandler;
import org.apache.hadoop.ipc.RefreshResponse;

/**
 * Refresh configs related to lock metrics in
 * {@link org.apache.hadoop.hdfs.server.namenode.FSNamesystemLock}
 */
public class FSNamesystemLockMetricsRefreshHandler implements RefreshHandler {

  public static final String FSN_LOCK_METRICS_REFRESH_HANDLER_IDENTIFIER =
      "RefreshFSNamesystemLockMetricsConfigs";
  private final FSNamesystem fsNamesystem;

  public FSNamesystemLockMetricsRefreshHandler(FSNamesystem fsNamesystem) {
    this.fsNamesystem = fsNamesystem;
  }

  @Override
  public RefreshResponse handleRefresh(String identifier, String[] args) {
    if (identifier.equals(FSN_LOCK_METRICS_REFRESH_HANDLER_IDENTIFIER)) {
      fsNamesystem.refreshLockMetricsConfigs(new Configuration());
      return RefreshResponse.successResponse();
    }
    return new RefreshResponse(-1, "Failed");
  }
}