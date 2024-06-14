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
package org.apache.hadoop.tools.federation;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.MultipleDestinationMountTableResolver;
import org.apache.hadoop.hdfs.server.federation.resolver.order.DestinationOrder;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.hdfs.server.federation.router.RouterClient;
import org.apache.hadoop.hdfs.server.federation.store.StateStoreService;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.createMountTableEntry;
import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.getAdminClient;
import static org.apache.hadoop.tools.federation.NSMigrationTool.JobStage.COPY;
import static org.apache.hadoop.tools.federation.NSMigrationTool.JobStage.FINISH;
import static org.apache.hadoop.tools.federation.NSMigrationTool.JobStage.MOUNT;
import static org.apache.hadoop.tools.federation.NSMigrationTool.JobStage.POST_COPY;
import static org.apache.hadoop.tools.federation.NSMigrationTool.JobStage.POST_FINISH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestNSMigrationTool {
  final private static int NUM_SUBCLUSTERS = 2;
  private static StateStoreDFSCluster cluster;
  private static MiniRouterDFSCluster.RouterContext routerContext;
  private static DFSClient routerClient;
  private static String routerAdminAddress;
  private static RouterClient routerAdmin;
  private static MiniRouterDFSCluster.NamenodeContext nnContext0;
  private static MiniRouterDFSCluster.NamenodeContext nnContext1;
  private static FileSystem nnFs0;
  private static FileSystem nnFs1;
  private static FileSystem routerFS;

  @BeforeClass
  public static void setup() throws Exception {
    cluster = new StateStoreDFSCluster(false, NUM_SUBCLUSTERS,
        MultipleDestinationMountTableResolver.class);
    Configuration conf = new RouterConfigBuilder().stateStore().heartbeat().admin().rpc().build();
    conf.set(RBFConfigKeys.DFS_ROUTER_MONITOR_NAMENODE, "ns0,ns1");
    conf.setInt(CommonConfigurationKeys.FS_TRASH_INTERVAL_KEY, (int) 1E9);
    conf.setStrings(CommonConfigurationKeys.FS_TRASH_ROOT, "/Trash");
    conf.setBoolean(RBFConfigKeys.MOUNT_TABLE_CACHE_UPDATE, true);
    cluster.addRouterOverrides(conf);
    cluster.startCluster();
    cluster.startRouters();
    cluster.waitClusterUp(true);
    routerContext = cluster.getRandomRouter();
    routerContext.getRouter().getStateStore().stopCacheUpdateService();
    routerAdminAddress = routerContext.getRouter().getAdminServerAddress().toString().split("/")[1];
    routerClient = routerContext.getClient();
    routerAdmin = getAdminClient(routerContext.getRouter());
    routerFS = routerContext.getFileSystem();
    nnContext0 = cluster.getNamenode("ns0", null);
    nnContext1 = cluster.getNamenode("ns1", null);
    nnFs0 = nnContext0.getFileSystem();
    nnFs1 = nnContext1.getFileSystem();
  }

  @AfterClass
  public static void cleanup() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  public void testReadWriteContext() throws IOException {
    String testPath = "/" + GenericTestUtils.getMethodName() + "0";
    NSMigrationTool.MigrationJob jobW =
        new NSMigrationTool.MigrationJob(new Path(testPath), "ns0", "ns1", routerContext.getConf(),
            routerAdminAddress, false);
    jobW.writeContext();

    NSMigrationTool.MigrationJob jobR =
        new NSMigrationTool.MigrationJob(new Path(testPath), "ns0", "ns1", routerContext.getConf(),
            routerAdminAddress, false);
    assertEquals(routerContext.getConf().toString(), jobR.getContext().conf.toString());
    assertEquals(jobR.getContext(), jobW.getContext());
  }

  private NSMigrationTool.MigrationJob setupTest(boolean continueJob, Path path,
      NSMigrationTool.JobStage runUntil, boolean createFile) throws Exception {
    routerClient.mkdirs(path.toString());
    NSMigrationTool.MigrationJob job =
        new NSMigrationTool.MigrationJob(path, "ns0", "ns1", routerContext.getConf(),
            routerAdminAddress, false);
    if (createFile) {
      FSDataOutputStream os = nnFs0.create(new Path(path, "tempFile"), true);
      os.writeUTF("TEST DATA");
      os.close();
    }

    while (job.getStage().stageInt < runUntil.stageInt) {
      job.writeContext();
      if (continueJob && job.getStage().stageInt == runUntil.stageInt - 1) {
        // Only simulate an interrupted stage during the tested stage, previous stages should pass
        NSMigrationTool.toggleInterruptForTesting(true);
        try {
          job.handleStage();
        } catch (RuntimeException ignored) {
        }
        NSMigrationTool.toggleInterruptForTesting(false);
        // Interrupt stage and retry stage again to ensure no issue would happen when stage is rerun
        job = new NSMigrationTool.MigrationJob(path, "ns0", "ns1", routerContext.getConf(),
            routerAdminAddress, false);
      }
      job.handleStage();
      if (!job.proceedToNextStage()) {
        return job;
      }
    }
    return job;
  }

  // Stage 1 is pretty much stateless, no need to test with resume
  @Test
  public void testJobStage1() throws Exception {
    GenericTestUtils.LogCapturer logs =
        GenericTestUtils.LogCapturer.captureLogs(LoggerFactory.getLogger(NSMigrationTool.class));
    String basePathStr = "/testStage1";
    Path basePath = new Path(basePathStr);
    routerClient.mkdirs(basePath.toString());
    createMountTableEntry(routerContext.getRouter(), basePathStr, DestinationOrder.FIXED,
        cluster.getNameservices());
    Path testPath;

    // Directory is empty
    testPath = new Path(basePath, "EmptyDir");
    NSMigrationTool.MigrationJob job1 =
        setupTest(false, testPath, NSMigrationTool.JobStage.MOUNT, false);
    assertTrue(logs.getOutput().contains("Nothing to migrate"));
    assertEquals(FINISH, job1.getStage());
    logs.clearOutput();

    // Existing mount point
    testPath = new Path(basePath, "ExistingMountPoint");
    createMountTableEntry(routerContext.getRouter(), testPath.toString(), DestinationOrder.FIXED,
        cluster.getNameservices());
    NSMigrationTool.MigrationJob job2 = setupTest(false, testPath, NSMigrationTool.JobStage.MOUNT,
        true);
    assertTrue(logs.getOutput().contains("Cannot initiate migration on existing mount point"));
    assertEquals(FINISH, job2.getStage());
    logs.clearOutput();

    // Existing mount points
    testPath = new Path(basePath, "NestedMountPoint");
    createMountTableEntry(routerContext.getRouter(), new Path(testPath, "inner").toString(),
        DestinationOrder.FIXED, cluster.getNameservices());
    NSMigrationTool.MigrationJob job3 =
        setupTest(false, testPath, NSMigrationTool.JobStage.MOUNT, true);
    assertTrue(logs.getOutput().contains("Cannot initiate migration on existing mount point"));
    assertEquals(FINISH, job3.getStage());
    logs.clearOutput();

    // New mount point
    testPath = new Path(basePath, "NewMountPoint");
    NSMigrationTool.MigrationJob job4 =
        setupTest(false, testPath, NSMigrationTool.JobStage.MOUNT, true);
    assertEquals(MOUNT, job4.getStage());
  }

  @Test
  public void testJobStage2MountNormal() throws Exception {
    testJobStage2Mount(false);
  }

  @Test
  public void testJobStage2MountResume() throws Exception {
    testJobStage2Mount(true);
  }

  public void testJobStage2Mount(boolean continueJob) throws Exception {
    String basePathStr;
    if (continueJob) {
      basePathStr = "/testStage2R";
    } else {
      basePathStr = "/testStage2N";
    }
    Path basePath = new Path(basePathStr);
    routerClient.mkdirs(basePath.toString());

    Path testPath = new Path(basePath, "NewMountPoint");
    NSMigrationTool.MigrationJob job = setupTest(continueJob, testPath, COPY, true);
    assertEquals(COPY, job.getStage());
    MountTable mountTable = getMountTableEntry(testPath.toString());
    assertTrue(mountTable.isReadOnly());
    assertEquals(1, mountTable.getDestinations().size());
    assertEquals("ns0", mountTable.getDestinations().get(0).getNameserviceId());
    String testFile = new Path(testPath, "file").toString();
    assertReadonly(routerClient, testFile);
  }

  @Test
  public void testJobStage3MigrateNormal() throws Exception {
    testJobStage3Migrate(false);
  }

  @Test
  public void testJobStage3MigrateResume() throws Exception {
    testJobStage3Migrate(true);
  }

  public void testJobStage3Migrate(boolean continueJob) throws Exception {
    String basePathStr;
    if (continueJob) {
      basePathStr = "/testStage3R";
    } else {
      basePathStr = "/testStage3N";
    }
    Path basePath = new Path(basePathStr);
    routerClient.mkdirs(basePath.toString());

    Path testPath = new Path(basePath, "NewMountPoint");
    NSMigrationTool.MigrationJob job = setupTest(continueJob, testPath, POST_COPY, true);
    assertEquals(POST_COPY, job.getStage());
    assertFalse(job.getContext().jobID.isEmpty());
    assertTrue(nnFs1.exists(new Path(testPath, "tempFile")));
    assertEquals("TEST DATA", nnFs1.open(new Path(testPath, "tempFile")).readUTF());
  }


  @Test
  public void testJobStage4CleanupNormal() throws Exception {
    testJobStage4Cleanup(false);
  }

  @Test
  public void testJobStage4CleanupResume() throws Exception {
    testJobStage4Cleanup(true);
  }

  private void testJobStage4Cleanup(boolean continueJob) throws Exception {
    String basePathStr;
    if (continueJob) {
      basePathStr = "/testStage4R";
    } else {
      basePathStr = "/testStage4N";
    }
    Path basePath = new Path(basePathStr);
    routerClient.mkdirs(basePath.toString());

    final Path testPath = new Path(basePath, "NewMountPoint");
    NSMigrationTool.MigrationJob job = setupTest(continueJob, testPath, FINISH, true);
    assertEquals(FINISH, job.getStage());
    // Mount point still exists but should point to ns1 now
    MountTable mountTable = getMountTableEntry(testPath.toString());
    assertTrue(mountTable.isReadOnly());
    assertEquals(1, mountTable.getDestinations().size());
    assertEquals("ns1", mountTable.getDestinations().get(0).getNameserviceId());
    String testFile = new Path(testPath, "file").toString();
    assertReadonly(routerClient, testFile);
    // Data should no longer exist on ns0
    assertFalse(nnFs0.exists(testPath));
    // and now exist in trash
    assertTrue(nnFs0.exists(nnFs0.getTrashRoot(testPath)));
  }

  @Test
  public void testJobStage5FinishNormal() throws Exception {
    testJobStage5Finish(false);
  }

  @Test
  public void testJobStage5FinishResume() throws Exception {
    testJobStage5Finish(true);
  }

  private void testJobStage5Finish(boolean continueJob) throws Exception {
    String basePathStr;
    if (continueJob) {
      basePathStr = "/testStage5R";
    } else {
      basePathStr = "/testStage5N";
    }
    Path basePath = new Path(basePathStr);
    routerClient.mkdirs(basePath.toString());

    final Path testPath = new Path(basePath, "NewMountPoint");
    NSMigrationTool.MigrationJob job = setupTest(continueJob, testPath, POST_FINISH, true);
    assertEquals(FINISH, job.getStage());
    // Mount point should no longer exist
    assertThrows(AssertionError.class, ()-> getMountTableEntry(testPath.toString()));
    assertTrue(nnFs1.exists(new Path(testPath, "tempFile")));
    // Context files should be deleted
    assertFalse(nnFs1.exists(job.getContext().contextPath));
  }

  private MountTable getMountTableEntry(String mountPoint) throws IOException {
    StateStoreService stateStore = routerContext.getRouter().getStateStore();
    stateStore.refreshCaches(true);
    GetMountTableEntriesRequest getRequest = GetMountTableEntriesRequest.newInstance(mountPoint);
    GetMountTableEntriesResponse getResponse =
        routerAdmin.getMountTableManager().getMountTableEntries(getRequest);
    List<MountTable> entries = getResponse.getEntries();
    assertEquals("Need exactly 1 entry: " + entries, 1, entries.size());
    assertEquals(mountPoint, entries.get(0).getSourcePath());
    return entries.get(0);
  }

  private void assertReadonly(DFSClient client, String path) throws IOException {
    try {
      client.create(path, true).close();
      fail("Should throw RemoteException");
    } catch (RemoteException e) {
      IOException ioe = e.unwrapRemoteException();
      assertTrue(ioe.getMessage().contains(path + " is in a read only mount point"));
    }
  }
}
