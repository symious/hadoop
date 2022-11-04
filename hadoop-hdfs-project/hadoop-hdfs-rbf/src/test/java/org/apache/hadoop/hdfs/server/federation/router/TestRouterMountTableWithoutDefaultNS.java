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
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableManager;
import org.apache.hadoop.hdfs.server.federation.resolver.MountTableResolver;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test a router end-to-end including the MountTable without default nameservice.
 */
public class TestRouterMountTableWithoutDefaultNS {

  private static StateStoreDFSCluster cluster;
  private static MiniRouterDFSCluster.RouterContext routerContext;
  private static MountTableResolver mountTable;
  private static ClientProtocol routerProtocol;
  private static FileSystem nnFs0;
  private static FileSystem nnFs1;

  @BeforeClass
  public static void globalSetUp() throws Exception {
    // Build and start a federated cluster
    cluster = new StateStoreDFSCluster(false, 2);
    Configuration conf = new RouterConfigBuilder()
        .stateStore()
        .admin()
        .rpc()
        .build();
    conf.setBoolean(RBFConfigKeys.DFS_ROUTER_DEFAULT_NAMESERVICE_ENABLE, false);
    cluster.addRouterOverrides(conf);
    cluster.startCluster();
    cluster.startRouters();
    cluster.waitClusterUp();

    // Get the end points
    nnFs0 = cluster.getNamenode("ns0", null).getFileSystem();
    nnFs1 = cluster.getNamenode("ns1", null).getFileSystem();
    routerContext = cluster.getRandomRouter();
    Router router = routerContext.getRouter();
    routerProtocol = routerContext.getClient().getNamenode();
    mountTable = (MountTableResolver) router.getSubclusterResolver();
  }

  @AfterClass
  public static void tearDown() {
    if (cluster != null) {
      cluster.stopRouter(routerContext);
      cluster.shutdown();
      cluster = null;
    }
  }

  @After
  public void clearMountTable() throws IOException {
    RouterClient client = routerContext.getAdminClient();
    MountTableManager mountTableManager = client.getMountTableManager();
    GetMountTableEntriesRequest req1 = GetMountTableEntriesRequest.newInstance("/");
    GetMountTableEntriesResponse response = mountTableManager.getMountTableEntries(req1);
    for (MountTable entry : response.getEntries()) {
      RemoveMountTableEntryRequest req2 =
          RemoveMountTableEntryRequest.newInstance(entry.getSourcePath());
      mountTableManager.removeMountTableEntry(req2);
    }
  }

  /**
   * Add a mount table entry to the mount table through the admin API.
   * @param entry Mount table entry to add.
   * @return If it was succesfully added.
   * @throws IOException Problems adding entries.
   */
  private boolean addMountTable(final MountTable entry) throws IOException {
    RouterClient client = routerContext.getAdminClient();
    MountTableManager mountTableManager = client.getMountTableManager();
    AddMountTableEntryRequest addRequest = AddMountTableEntryRequest.newInstance(entry);
    AddMountTableEntryResponse addResponse = mountTableManager.addMountTableEntry(addRequest);

    // Reload the Router cache
    mountTable.loadCache(true);

    return addResponse.getStatus();
  }

  @Test
  public void testGetContentSummary() throws Exception {
    try {
      // Add mount table entry.
      MountTable addEntry = MountTable.newInstance("/testA",
          Collections.singletonMap("ns0", "/testA"));
      assertTrue(addMountTable(addEntry));
      addEntry = MountTable.newInstance("/testA/testB",
          Collections.singletonMap("ns0", "/testA/testB"));
      assertTrue(addMountTable(addEntry));
      addEntry = MountTable.newInstance("/testA/testB/testC",
          Collections.singletonMap("ns1", "/testA/testB/testC"));
      assertTrue(addMountTable(addEntry));

      writeData(nnFs0, new Path("/testA/testB/file1"), 1024 * 1024);
      writeData(nnFs1, new Path("/testA/testB/testC/file2"), 1024 * 1024);
      writeData(nnFs1, new Path("/testA/testB/testC/file3"), 1024 * 1024);

      RouterRpcServer routerRpcServer = routerContext.getRouterRpcServer();
      ContentSummary summary = routerRpcServer.getContentSummary("/testA");
      assertEquals(3, summary.getFileCount());
      assertEquals(1024 * 1024 * 3, summary.getLength());

      LambdaTestUtils.intercept(NoLocationException.class,
          () -> routerRpcServer.getContentSummary("/testB"));
    } finally {
      nnFs0.delete(new Path("/testA"), true);
      nnFs1.delete(new Path("/testA"), true);
    }
  }

  void writeData(FileSystem fs, Path path, int fileLength) throws IOException {
    try (FSDataOutputStream outputStream = fs.create(path)) {
      for (int writeSize = 0; writeSize < fileLength; writeSize++) {
        outputStream.write(writeSize);
      }
    }
  }
}
