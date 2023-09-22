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
package org.apache.hadoop.hdfs.server.federation.router;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.metrics.FederationRPCMetrics;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeContext;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeServiceState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.NAMENODES;
import static org.junit.Assert.assertEquals;

public class TestMSyncForNewConnection {
  private static MiniRouterDFSCluster cluster;

  @Before
  public void startUpCluster() throws Exception {
    Configuration conf = new Configuration(false);
    conf.setBoolean(RBFConfigKeys.DFS_ROUTER_OBSERVER_READ_ENABLE, true);
    conf.setInt(RBFConfigKeys.DFS_ROUTER_OBSERVER_AUTO_MSYNC_PERIOD, 1000000);
    conf.setBoolean(DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_KEY, true);
    conf.setBoolean(DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_OBSERVER_KEY, true);
    conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 0);
    conf.setInt(DFSConfigKeys.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY, 3000);

    cluster = new MiniRouterDFSCluster(true, 1, 3);
    cluster.addNamenodeOverrides(conf);
    cluster.addRouterOverrides(conf);
    // Start NNs and DNs and wait until ready
    cluster.startCluster(true);

    // Start routers with only an RPC service
    cluster.startRouters();

    // Register and verify all NNs with all routers
    cluster.registerNamenodes();
    cluster.waitNamenodeRegistration();
    // Setup the mount table
    cluster.installMockLocations();

    // Making one Namenodes active per nameservice
    if (cluster.isHighAvailability()) {
      for (String ns : cluster.getNameservices()) {
        cluster.switchToActive(ns, NAMENODES[0]);
        cluster.switchToStandby(ns, NAMENODES[1]);
        cluster.switchToObserver(ns, NAMENODES[2]);
      }
    }
    cluster.waitActiveNamespaces();
    cluster.waitObserverNamespaces(1);
  }

  @After
  public void tearDown() throws IOException {
    if (cluster != null) {
      cluster.shutdownWithObserver();
      cluster = null;
    }
  }

  @Test
  public void testMsyncForNewConnection() throws Exception {
    MiniRouterDFSCluster.RouterContext routerContext = cluster.getRandomRouter();
    List<? extends FederationNamenodeContext> namenodes = routerContext
        .getRouter().getNamenodeResolver()
        .getNamenodesForNameserviceId(cluster.getNameservices().get(0), true);
    assertEquals("First namenode should be observer",
        FederationNamenodeServiceState.OBSERVER, namenodes.get(0).getState());

    FederationRPCMetrics rpcMetrics = routerContext.getRouter().getRpcServer()
        .getRPCMetrics();

    FileSystem fileSystem = routerContext.getFileSystem();
    Path path = new Path("/testFile");
    // Send Create call to active
    fileSystem.create(path).close();

    long rpcCountForActive = rpcMetrics.getProxyOpActiveCommunicate();

    // with msync
    fileSystem.open(path).close();
    long rpcCountForActive1 = rpcMetrics.getProxyOpActiveCommunicate();
    assertEquals(rpcCountForActive + 1, rpcCountForActive1);

    // without msync
    fileSystem.open(path).close();
    fileSystem.open(path).close();
    fileSystem.open(path).close();
    fileSystem.open(path).close();
    fileSystem.open(path).close();
    long rpcCountForActive2 = rpcMetrics.getProxyOpActiveCommunicate();
    assertEquals(rpcCountForActive1, rpcCountForActive2);

    // Sleep 10s to let the old connection timeout.
    Thread.sleep(10000);

    // new connection with msync
    fileSystem.open(path).close();
    long rpcCountForActive3 = rpcMetrics.getProxyOpActiveCommunicate();
    assertEquals(rpcCountForActive2 + 1, rpcCountForActive3);

    // old connection without msync
    fileSystem.open(path).close();
    fileSystem.open(path).close();
    fileSystem.open(path).close();
    fileSystem.open(path).close();
    fileSystem.open(path).close();
    long rpcCountForActive4 = rpcMetrics.getProxyOpActiveCommunicate();
    assertEquals(rpcCountForActive3, rpcCountForActive4);
  }
}
