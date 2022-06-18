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

package org.apache.hadoop.hdfs.server.federation.router.handler;

import org.apache.hadoop.hdfs.server.federation.router.RouterAdminServer;
import org.apache.hadoop.ipc.RefreshHandler;
import org.apache.hadoop.ipc.RefreshResponse;

public class RefreshConfiguredNamenodesHandler implements RefreshHandler {

  final static public String REFRESH_CONFIGURED_NAMENODES_HANDLER_IDENTIFIER = "RefreshConfiguredNamenodes";
  private final RouterAdminServer routerAdminServer;

  public RefreshConfiguredNamenodesHandler(RouterAdminServer routerAdminServer) {
    this.routerAdminServer = routerAdminServer;
  }

  @Override
  public RefreshResponse handleRefresh(String identifier, String[] args) {
    if (REFRESH_CONFIGURED_NAMENODES_HANDLER_IDENTIFIER.equals(identifier)) {
      return new RefreshResponse(0,
          routerAdminServer.refreshNameservicesAndNamenodes());
    }
    return new RefreshResponse(-1, "Failed");
  }
}
