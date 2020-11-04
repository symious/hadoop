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

import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.FEDERATION_STORE_DRIVER_CLASS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableManager;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver;
import org.apache.hadoop.hdfs.server.federation.store.driver.impl.StateStoreZooKeeperImpl;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RefreshMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RefreshMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.hdfs.server.federation.store.records.RouterState;
import org.apache.hadoop.util.Time;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The administrator interface of the {@link Router} implemented by
 * {@link RouterAdminServer}.
 */
public class TestRouterAdminWithZKStateStore {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestRouterAdminWithZKStateStore.class);

  private static TestingServer curatorTestingServer;
  private static StateStoreDFSCluster cluster;
  private static RouterContext routerContext;
  public static final String RPC_BEAN =
      "Hadoop:service=Router,name=FederationRPC";

  @BeforeClass
  public static void setUp() throws Exception {
    curatorTestingServer = new TestingServer();
    curatorTestingServer.start();
    final String connectString = curatorTestingServer.getConnectString();
    int numNameservices = 2;
    cluster = new StateStoreDFSCluster(true, numNameservices) {

      @Override
      public Configuration generateRouterConfiguration(String nsId,
          String nnId) {
        Configuration conf = super.generateRouterConfiguration(nsId, nnId);
        conf.set(CommonConfigurationKeys.ZK_ADDRESS, connectString);
        conf.setClass(FEDERATION_STORE_DRIVER_CLASS,
            StateStoreZooKeeperImpl.class, StateStoreDriver.class);
        return conf;
      }

    };
    // Build and start a router with State Store + admin + RPC
    Configuration conf = new RouterConfigBuilder().stateStore().admin().rpc()
        .heartbeat().build();
    cluster.addRouterOverrides(conf);
    cluster.startRouters();
    routerContext = cluster.getRandomRouter();
    // wait for one minute for all the routers to get registered
    assertTrue("Router registration failed",
        waitForRouterRegistration(numNameservices * 2, 60000));
  }

  @AfterClass
  public static void tearDown() {
    try {
      curatorTestingServer.close();
      cluster.shutdown();
    } catch (IOException e) {
      // do noting
    }
  }

  /**
   * addMountTableEntry API should internally update the cache on all the
   * routers.
   */
  @Test
  public void testMountTableEntriesCacheUpdatedAfterAddAPICall()
      throws IOException {

    MountTableManager mountTableManager =
        routerContext.getAdminClient().getMountTableManager();

    // Existing mount table size
    int existingEntriesCount = getCount();
    String srcPath = "/addPath";
    MountTable newEntry = MountTable.newInstance(srcPath,
        Collections.singletonMap("ns0", "/addPathDest"), Time.now(),
        Time.now());
    add(mountTableManager, newEntry);

    // When Add entry is done, all the routers must have updated its mount table
    // entry
    List<RouterContext> routers = cluster.getRouters();
    for (RouterContext rc : routers) {
      List<MountTable> result = getMountTableEntries(rc);
      assertEquals(1 + existingEntriesCount, result.size());
      MountTable mountTableResult = result.get(0);
      assertEquals(srcPath, mountTableResult.getSourcePath());
    }
  }

  /**
   * removeMountTableEntry API should internally update the cache on all the
   * routers.
   */
  @Test
  public void testMountTableEntriesCacheUpdatedAfterRemoveAPICall()
      throws IOException {

    MountTableManager mountTableManager =
        routerContext.getAdminClient().getMountTableManager();

    // Existing mount table size
    int initialCount = getCount();

    // add
    String srcPath = "/removePathSrc";
    MountTable newEntry = MountTable.newInstance(srcPath,
        Collections.singletonMap("ns0", "/removePathDest"), Time.now(),
        Time.now());
    add(mountTableManager, newEntry);
    int addCount = getCount();
    assertEquals(initialCount + 1, addCount);

    // remove
    RemoveMountTableEntryResponse removeMountTableEntry =
        mountTableManager.removeMountTableEntry(
            RemoveMountTableEntryRequest.newInstance(srcPath));
    assertTrue(removeMountTableEntry.getStatus());

    int removeCount = getCount();
    assertEquals(addCount - 1, removeCount);
  }

  /**
   * updateMountTableEntry API should internally update the cache on all the
   * routers.
   */
  @Test
  public void testMountTableEntriesCacheUpdatedAfterUpdateAPICall()
      throws IOException {

    MountTableManager mountTableManager =
        routerContext.getAdminClient().getMountTableManager();

    // Existing mount table size
    int initialCount = getCount();

    // add
    String srcPath = "/updatePathSrc";
    MountTable newEntry = MountTable.newInstance(srcPath,
        Collections.singletonMap("ns0", "/updatePathDest"), Time.now(),
        Time.now());
    add(mountTableManager, newEntry);
    int addCount = getCount();
    assertEquals(initialCount + 1, addCount);

    // update
    String key = "ns2";
    String value = "/updatePathDest2";
    MountTable upateEntry = MountTable.newInstance(srcPath,
        Collections.singletonMap(key, value), Time.now(), Time.now());
    UpdateMountTableEntryResponse updateMountTableEntry =
        mountTableManager.updateMountTableEntry(
            UpdateMountTableEntryRequest.newInstance(upateEntry));
    assertTrue(updateMountTableEntry.getStatus());
    MountTable updatedMountTable = getMountTable(srcPath);
    assertNotNull("Updated mount table entrty can not be null",
        updatedMountTable);
    assertEquals(1, updatedMountTable.getDestinations().size());
    assertEquals(key,
        updatedMountTable.getDestinations().get(0).getNameserviceId());
    assertEquals(value, updatedMountTable.getDestinations().get(0).getDest());
  }

  @Test
  public void testRefreshMountTableEntriesAPI() throws IOException {

    MountTableManager mountTableManager =
        routerContext.getAdminClient().getMountTableManager();
    RefreshMountTableEntriesRequest request =
        RefreshMountTableEntriesRequest.newInstance();
    RefreshMountTableEntriesResponse refreshMountTableEntriesRes =
        mountTableManager.refreshMountTableEntries(request);
    // refresh should be successful
    assertTrue(refreshMountTableEntriesRes.getResult());
  }

  private int getCount() throws IOException {
    List<MountTable> records = getMountTableEntries();
    int oldEntriesCount = records.size();
    return oldEntriesCount;
  }

  private MountTable getMountTable(String srcPath) throws IOException {
    List<MountTable> mountTableEntries = getMountTableEntries();
    for (MountTable mountTable : mountTableEntries) {
      String sourcePath = mountTable.getSourcePath();
      if (srcPath.equals(sourcePath)) {
        return mountTable;
      }
    }
    return null;
  }

  private void add(MountTableManager mountTableManager, MountTable newEntry)
      throws IOException {
    AddMountTableEntryRequest addRequest =
        AddMountTableEntryRequest.newInstance(newEntry);
    AddMountTableEntryResponse addResponse =
        mountTableManager.addMountTableEntry(addRequest);
    assertTrue(addResponse.getStatus());
  }

  private List<MountTable> getMountTableEntries() throws IOException {
    return getMountTableEntries(routerContext);
  }

  private List<MountTable> getMountTableEntries(
      RouterContext routerContextParam) throws IOException {
    GetMountTableEntriesRequest request =
        GetMountTableEntriesRequest.newInstance("/");
    MountTableManager mountTableManager =
        routerContextParam.getAdminClient().getMountTableManager();
    return mountTableManager.getMountTableEntries(request).getEntries();
  }

  private static boolean waitForRouterRegistration(int numberOfRouters,
      long timeout) {
    long start = System.currentTimeMillis();
    while (true) {
      try {
        List<RouterState> cachedRecords = routerContext.getRouter()
            .getRouterStateManager().getCachedRecords();
        if (cachedRecords.size() == numberOfRouters) {
          return true;
        }
      } catch (IOException e) {
        // ignore as this is expected
        LOG.info("Error while fetching RouterState records", e);
      }
      if (System.currentTimeMillis() > start + timeout) {
        break;
      }
      try {
        Thread.sleep(250);
      } catch (InterruptedException e) {
        // do nothing
      }
    }
    return false;
  }
}