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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamenodeContext;
import org.apache.hadoop.hdfs.server.federation.resolver.FederationNamespaceInfo;
import org.apache.hadoop.hdfs.server.federation.resolver.MembershipNamenodeResolver;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreService;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.test.Whitebox;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.apache.hadoop.hdfs.server.federation.store.FederationStateStoreTestUtils.synchronizeRecords;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestRouterNamenodesRefresh {

  private static StateStoreDFSCluster cluster;
  private static MiniRouterDFSCluster.RouterContext routerContext;
  private static MembershipNamenodeResolver resolver;

  @Before
  public void globalSetUp() throws Exception {
    cluster = new StateStoreDFSCluster(true, 3);
    // Build and start a router with State Store + admin + RPC + monitoring
    Configuration conf =
        new RouterConfigBuilder().stateStore().admin().heartbeat().rpc()
            .build();
    conf.setLong(RBFConfigKeys.FEDERATION_STORE_MEMBERSHIP_EXPIRATION_MS, 2000);
    conf.setLong(RBFConfigKeys.DFS_ROUTER_HEARTBEAT_INTERVAL_MS, 200);
    conf.set(RBFConfigKeys.DFS_ROUTER_MONITOR_NAMENODE,
        "ns0.nn0,ns1.nn0,ns1.nn1");
    cluster.addRouterOverrides(conf);
    cluster.startCluster();
    cluster.startRouters();
    cluster.waitClusterUp(true);

    routerContext = cluster.getRandomRouter();
    List<MountTable> mockMountTable = cluster.generateMockMountTable();
    Router router = routerContext.getRouter();
    StateStoreService stateStore = router.getStateStore();
    stateStore.refreshCaches(true);
    resolver = (MembershipNamenodeResolver) routerContext.getRouter()
        .getNamenodeResolver();

    RouterRpcServer spyRpcServer =
        Mockito.spy(routerContext.getRouter().createRpcServer());
    Whitebox.setInternalState(routerContext.getRouter(), "rpcServer",
        spyRpcServer);
    Mockito.doReturn(null).when(spyRpcServer).getFileInfo(Mockito.anyString());

    assertTrue(
        synchronizeRecords(stateStore, mockMountTable, MountTable.class));
    // Avoid running with random users
    routerContext.resetAdminClient();
  }

  @After
  public void tearDown() {
    cluster.stopRouter(routerContext);
    cluster.shutdown();
  }

  @Test
  public void testMultipleRefreshes() {
    // Start: ns0.nn0,ns1.nn0,ns1.nn1
    // Next: ns0.nn0,ns0.nn1,ns1.nn0,ns1.nn1,ns2.nn0,ns2.nn1
    String result = refreshConfiguredNamenodes("ns2.nn0,ns2.nn1,ns0.nn1", true);
    assertEquals("Added=ns0:nn1;ns2:nn1,nn0|Removed=", result);

    // Next: ns1.nn1,ns2.nn0,ns2.nn1
    result = refreshConfiguredNamenodes("ns1.nn1,ns2.nn0,ns2.nn1", false);
    assertEquals("Added=|Removed=ns0:nn1,nn0;ns1:nn0", result);

    // Next: ns0.nn1,ns2.nn0,ns2.nn1
    result = refreshConfiguredNamenodes("ns0.nn1,ns2.nn0,ns2.nn1", false);
    assertEquals("Added=ns0:nn1|Removed=ns1:nn1", result);
  }

  @Test
  public void testRefreshNewNamenodes() throws IOException {
    List<? extends FederationNamenodeContext> namespaceInfo0 =
        resolver.getNamenodesForNameserviceId("ns0", false);

    // Before refresh, only 1 nn for ns0
    assertEquals(1, namespaceInfo0.size());

    refreshConfiguredNamenodes("ns0.nn1", true);

    Collection<NamenodeHeartbeatService> heartbeatServices =
        routerContext.getRouter().getNamenodeHearbeatServices();
    for (NamenodeHeartbeatService service : heartbeatServices) {
      service.periodicInvoke();
    }

    // Force refresh cache. Can also just sleep for a few seconds.
    resolver.loadCache(true);

    // After refresh, got 2 namenodes for ns0
    namespaceInfo0 = resolver.getNamenodesForNameserviceId("ns0", false);
    assertEquals(2, namespaceInfo0.size());
    assertTrue(verifyNamenodes(namespaceInfo0, new String[] { "nn0", "nn1" }));
  }

  @Test
  public void testRefreshNewNameservices() throws IOException {
    List<? extends FederationNamenodeContext> namespaceInfo2 =
        resolver.getNamenodesForNameserviceId("ns2", false);

    // Before refresh, no nns on ns2
    assertNull(namespaceInfo2);

    refreshConfiguredNamenodes("ns2.nn0,ns2.nn1", true);

    Collection<NamenodeHeartbeatService> heartbeatServices =
        routerContext.getRouter().getNamenodeHearbeatServices();
    for (NamenodeHeartbeatService service : heartbeatServices) {
      service.periodicInvoke();
    }

    resolver.loadCache(true);

    // After refresh, got 2 namenodes for ns2
    namespaceInfo2 = resolver.getNamenodesForNameserviceId("ns2", false);
    assertEquals(2, namespaceInfo2.size());
    assertTrue(verifyNamenodes(namespaceInfo2, new String[] { "nn0", "nn1" }));
  }

  @Test
  public void testRefreshNamenodesNamespacesRemoved() throws IOException {
    List<? extends FederationNamenodeContext> namespaceInfo0 =
        resolver.getNamenodesForNameserviceId("ns0", false);
    List<? extends FederationNamenodeContext> namespaceInfo1 =
        resolver.getNamenodesForNameserviceId("ns1", false);
    Set<FederationNamespaceInfo> nss = resolver.getNamespaces();

    // Before refresh, ns0.nn0, ns1.nn0, ns1.nn1
    assertEquals(1, namespaceInfo0.size());
    assertEquals(2, namespaceInfo1.size());
    assertEquals(2, nss.size());
    assertTrue(verifyNamenodes(namespaceInfo0, new String[] { "nn0" }));
    assertTrue(verifyNamenodes(namespaceInfo1, new String[] { "nn0", "nn1" }));

    refreshAllConfiguredNamenodes("ns0.nn1", false);

    Collection<NamenodeHeartbeatService> heartbeatServices =
        routerContext.getRouter().getNamenodeHearbeatServices();
    for (NamenodeHeartbeatService service : heartbeatServices) {
      service.periodicInvoke();
    }

    // Wait for 5 seconds so that old cache expires
    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    resolver.loadCache(true);

    namespaceInfo0 = resolver.getNamenodesForNameserviceId("ns0", false);
    namespaceInfo1 = resolver.getNamenodesForNameserviceId("ns1", false);
    nss = resolver.getNamespaces();

    // After refresh, ns0.nn1
    assertEquals(1, namespaceInfo0.size());
    assertNull(namespaceInfo1);
    assertTrue(verifyNamenodes(namespaceInfo0, new String[] { "nn1" }));
    assertEquals(1, nss.size());
  }

  @Test
  public void testConcurrentRefreshes()
      throws InterruptedException, IOException {
    // Start: ns0.nn0,ns1.nn0,ns1.nn1
    // Middle: random refreshes
    // End: ns0.nn0,ns0.nn1,ns1.nn0,ns1.nn1,ns2.nn0,ns2.nn1

    int initialServiceCount = routerContext.getRouter().getServices().size();
    final List<String> namenodes =
        Arrays.asList("ns0.nn0", "ns0.nn1", "ns1.nn0", "ns1.nn1", "ns2.nn0",
            "ns2.nn1");
    final Random rand = new Random();
    // Spawn 10 concurrent refresh requests
    Thread[] threads = new Thread[10];
    for (int i = 0; i < 10; i++) {
      threads[i] = new Thread(new Runnable() {
        @Override
        public void run() {
          refreshConfiguredNamenodes(
              Joiner.on(",").join(getRandomSublist(namenodes, rand.nextInt(6))),
              false);
        }
      });
    }

    for (Thread thread : threads) {
      thread.start();
    }

    for (Thread thread : threads) {
      thread.join();
    }

    refreshConfiguredNamenodes(
        "ns0.nn0,ns0.nn1,ns1.nn0,ns1.nn1,ns2.nn0,ns2.nn1", false);
    for (NamenodeHeartbeatService service : routerContext.getRouter()
        .getNamenodeHearbeatServices()) {
      service.periodicInvoke();
    }
    resolver.loadCache(true);

    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    assertEquals(6,
        routerContext.getRouter().getNamenodeHearbeatServices().size());
    assertEquals(initialServiceCount - 3 + 6,
        routerContext.getRouter().getServices().size());
    for (String ns : cluster.getNameservices()) {
      assertTrue(
          verifyNamenodes(resolver.getNamenodesForNameserviceId(ns, false),
              new String[] { "nn0", "nn1" }));
    }
  }

  private boolean verifyNamenodes(
      List<? extends FederationNamenodeContext> namespaceInfo,
      String[] namenodesToCheck) {
    Set<String> checkNnsSet = new HashSet<>(Arrays.asList(namenodesToCheck));
    for (FederationNamenodeContext nn : namespaceInfo) {
      if (!checkNnsSet.contains(nn.getNamenodeId())) {
        return false;
      }
      checkNnsSet.remove(nn.getNamenodeId());
    }
    return checkNnsSet.isEmpty();
  }

  private void refreshAllConfiguredNamenodes(String namenodes, boolean append) {
    for (MiniRouterDFSCluster.RouterContext context : cluster.getRouters()) {
      final RouterAdminServer admin =
          context.getRouter().getRouterAdminServer();
      final Configuration conf =
          new Configuration(context.getRouter().getConfig());
      final String oldNns = conf.get(RBFConfigKeys.DFS_ROUTER_MONITOR_NAMENODE);
      if (append) {
        conf.set(RBFConfigKeys.DFS_ROUTER_MONITOR_NAMENODE,
            oldNns + "," + namenodes);
      } else {
        conf.set(RBFConfigKeys.DFS_ROUTER_MONITOR_NAMENODE, namenodes);
      }
      admin.refreshNameservicesAndNamenodes(conf);
    }
  }

  private String refreshConfiguredNamenodes(String namenodes, boolean append) {
    final RouterAdminServer admin = routerContext.getRouter().getRouterAdminServer();
    final Configuration conf = new Configuration(routerContext.getRouter().getConfig());
    final String oldNns = conf.get(RBFConfigKeys.DFS_ROUTER_MONITOR_NAMENODE);
    if (append) {
      conf.set(RBFConfigKeys.DFS_ROUTER_MONITOR_NAMENODE,
          oldNns + "," + namenodes);
    } else {
      conf.set(RBFConfigKeys.DFS_ROUTER_MONITOR_NAMENODE, namenodes);
    }
    return admin.refreshNameservicesAndNamenodes(conf);
  }

  private List<String> getRandomSublist(List<String> original, int n) {
    Random rand = new Random();
    List<String> cloneList = new ArrayList<>(original);
    if (n >= original.size()) {
      return cloneList;
    }
    for (int i = 0; i < original.size() - n; i++) {
      cloneList.remove(cloneList.get(rand.nextInt(cloneList.size())));
    }
    return cloneList;
  }
}