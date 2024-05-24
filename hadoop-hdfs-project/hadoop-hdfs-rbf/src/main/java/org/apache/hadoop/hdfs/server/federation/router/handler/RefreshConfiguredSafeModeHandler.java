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

import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.federation.router.Router;
import org.apache.hadoop.ipc.RefreshHandler;
import org.apache.hadoop.ipc.RefreshResponse;

public class RefreshConfiguredSafeModeHandler implements RefreshHandler {

  public final static String REFRESH_CONFIGURED_SAFEMODE_HANDLER_IDENTIFIER =
      "RefreshConfiguredSafeMode";

  private final Router router;

  public RefreshConfiguredSafeModeHandler(Router router) {
    this.router = router;
  }

  @Override
  public RefreshResponse handleRefresh(String identifier, String[] args) {
    if (REFRESH_CONFIGURED_SAFEMODE_HANDLER_IDENTIFIER.equals(identifier)) {
      return new RefreshResponse(0, router.refreshSafeMode(new HdfsConfiguration()));
    }
    return new RefreshResponse(-1, "Failed");
  }
}
