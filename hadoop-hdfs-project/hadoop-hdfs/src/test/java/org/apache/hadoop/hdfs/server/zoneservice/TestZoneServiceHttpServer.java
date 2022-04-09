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
package org.apache.hadoop.hdfs.server.zoneservice;

import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.TestNameNodeHttpServer;
import org.apache.hadoop.http.HttpConfig;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.net.InetSocketAddress;

@RunWith(value = Parameterized.class)
public class TestZoneServiceHttpServer extends TestNameNodeHttpServer {

  public TestZoneServiceHttpServer(HttpConfig.Policy policy) {
    super(policy);
  }

  @Test
  public void testHttpPolicy() throws Exception {
    conf.set(DFSConfigKeys.DFS_HTTP_POLICY_KEY, policy.name());
    conf.set(DFSConfigKeys.DFS_ZONESERVICE_HTTPS_ADDRESS_KEY, "localhost:0");

    InetSocketAddress addr = InetSocketAddress.createUnresolved("localhost", 0);
    ZoneServiceHttpServer server = null;
    try {
      server = new ZoneServiceHttpServer(conf, addr);
      server.start();

      Assert.assertTrue(implies(policy.isHttpEnabled(),
          canAccess("http", server.getHttpAddress())));
      Assert.assertTrue(implies(!policy.isHttpEnabled(),
          server.getHttpAddress() == null));

      Assert.assertTrue(implies(policy.isHttpsEnabled(),
          canAccess("https", server.getHttpsAddress())));
      Assert.assertTrue(implies(!policy.isHttpsEnabled(),
          server.getHttpsAddress() == null));

    } finally {
      if (server != null) {
        server.stop();
      }
    }
  }
}
