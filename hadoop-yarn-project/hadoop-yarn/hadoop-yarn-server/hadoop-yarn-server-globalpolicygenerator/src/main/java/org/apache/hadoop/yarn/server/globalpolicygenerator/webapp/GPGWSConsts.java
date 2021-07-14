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

package org.apache.hadoop.yarn.server.globalpolicygenerator.webapp;

/**
 * Constants for GPGWSConsts
 */
public final class GPGWSConsts {

  public static final String ANY = "*";

  public static final String QUEUENAME = "queueName";

  public static final String POLICY_LIST = "/policy/list";

  public static final String POLICY_UPDATE = "/policy/update";

  public static final String WEIGHTS_FIELD = "routerPolicyWeights";

  public static final String ENTRY_FIELD = "entry";

  public static final String KEY_FIELD = "key";

  public static final String VALUE_FIELD = "value";

  public static final String ID_FIELD = "id";

  public static final String APP_HOME_IMPORT = "/app_home/import";

  public static final String CLUSTER_ID = "clusterId";


  private GPGWSConsts() {
    // not called
  }

}