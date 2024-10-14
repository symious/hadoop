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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.MultipleDestinationMountTableResolver;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.tools.federation.migration.AnalyzeJob;
import org.apache.hadoop.tools.federation.migration.MigrationJob;
import org.apache.hadoop.tools.federation.migration.ProjectJob;
import org.apache.hadoop.util.Time;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.tools.federation.migration.MigrationJob.toggleSkipTopTwoLevelsForTesting;
import static org.apache.hadoop.tools.federation.migration.MigrationUtils.loadPathsFromDfs;
import static org.apache.hadoop.tools.federation.migration.MigrationUtils.loadPathsWithCountFromDfs;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestNSMigrationToolBatch {
  final private static int NUM_SUBCLUSTERS = 2;
  private static StateStoreDFSCluster cluster;
  private static MiniRouterDFSCluster.RouterContext routerContext;
  private static String routerAdminAddress;
  private static MiniRouterDFSCluster.NamenodeContext nnContext0;
  private static MiniRouterDFSCluster.NamenodeContext nnContext1;
  private static FileSystem nnFs0;
  private static FileSystem nnFs1;

  @BeforeClass
  public static void setup() throws Exception {
    cluster = new StateStoreDFSCluster(false, NUM_SUBCLUSTERS,
        MultipleDestinationMountTableResolver.class);
    Configuration conf = new RouterConfigBuilder().stateStore().heartbeat().admin().rpc().build();
    conf.set(RBFConfigKeys.DFS_ROUTER_MONITOR_NAMENODE, "ns0,ns1");
    conf.setInt(CommonConfigurationKeys.FS_TRASH_INTERVAL_KEY, (int) 1E9);
    conf.setStrings(CommonConfigurationKeys.FS_TRASH_ROOT, "/Trash");
    conf.setBoolean(RBFConfigKeys.MOUNT_TABLE_CACHE_UPDATE, true);
    conf.setInt(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 3000);
    cluster.addRouterOverrides(conf);
    cluster.startCluster();
    cluster.startRouters();
    cluster.waitClusterUp(true);
    routerContext = cluster.getRandomRouter();
    routerContext.getRouter().getStateStore().stopCacheUpdateService();
    routerAdminAddress = routerContext.getRouter().getAdminServerAddress().toString().split("/")[1];
    nnContext0 = cluster.getNamenode("ns0", null);
    nnContext1 = cluster.getNamenode("ns1", null);
    nnFs0 = nnContext0.getFileSystem();
    nnFs1 = nnContext1.getFileSystem();
    toggleSkipTopTwoLevelsForTesting(false);
    MigrationJob.initializeFastCopyInstance(routerContext.getConf());
  }

  @AfterClass
  public static void cleanup() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
    System.gc();
  }

  @Test
  public void testBatch() throws Exception {
    Path basePath = new Path("/testBatch");
    setupTest(basePath, false);
    Path tempDirsList = new Path("/tmp/input.txt");

    new AnalyzeJob("/testBatch", "ns0", "ns1", "ns0", "10", tempDirsList, 1,
        routerContext.getConf()).execute();

    DistributedFileSystem fs = (DistributedFileSystem) routerContext.getFileSystem();
    Map<Path, Pair<Integer, Long>> coldPaths = loadPathsWithCountFromDfs(fs, tempDirsList);
    assertEquals(2 + 2 * 2, coldPaths.size());
    assertNotNull(coldPaths.get(new Path("/testBatch/0")));
    assertNotNull(coldPaths.get(new Path("/testBatch/1")));
    assertNotNull(coldPaths.get(new Path("/testBatch/2/0")));
    assertNotNull(coldPaths.get(new Path("/testBatch/2/1")));
    assertNotNull(coldPaths.get(new Path("/testBatch/3/0")));
    assertNotNull(coldPaths.get(new Path("/testBatch/3/1")));

    Path migratedPaths = new Path("/tmp/output.txt");
    Path ignorePath = new Path("/tmp/input.txt.ignore");

    // Cold migration
    MigrationJob.runBatchJob(routerContext.getConf(), "6", coldPaths, "ns0", "ns1",
        routerAdminAddress, false, ignorePath, migratedPaths, false, 0, 0, null);

    // Remaining data: One fully hot dir with 5 hot subdirs, 2 partially hot dirs with 3 hot subdirs
    assertEquals(5 + 3 * 2, nnFs0.getContentSummary(basePath).getFileCount());

    AnalyzeJob.listAllFilePaths(routerContext.getConf(), "ns0", "/testBatch", tempDirsList);
    Set<Path> hotPaths = loadPathsFromDfs(fs, basePath, tempDirsList);
    // Hot migration
    MigrationJob.runBatchJob(routerContext.getConf(), "6", hotPaths, "ns0", "ns1",
        routerAdminAddress, false, ignorePath, migratedPaths, true, 0, 0);

    // Everything has been moved
    assertEquals(25, nnFs1.getContentSummary(basePath).getFileCount());
  }

  @Test
  public void testProject() throws Exception {
    Path basePath = new Path("/projects/test");
    setupTest(basePath, false);
    // Cold run
    new ProjectJob(basePath.toString(), "test", "ns0", "ns1", "ns0", routerAdminAddress, 8, 6, 100,
        10, false, 0, 0, false, routerContext.getConf()).execute();
    // Hot run
    new ProjectJob(basePath.toString(), "test", "ns0", "ns1", "ns0", routerAdminAddress, 8, 6, 0,
        10, true, 0, 0, false, routerContext.getConf()).execute();
    assertEquals(25, nnFs1.getContentSummary(basePath).getFileCount());

    for (int i = 0; i < 5; i++) {
      Path outerPath = new Path(basePath, String.valueOf(i));
      for (int j = 0; j < 5; j++) {
        Path dirPath = new Path(outerPath, String.valueOf(j));
        Path filePath = new Path(dirPath, "file");
        FileStatus fileStatus = nnFs1.getFileStatus(filePath);
        assertEquals("alice", fileStatus.getOwner());
        assertEquals("bob", fileStatus.getGroup());
      }
    }
  }

  @Test
  public void testProjectWithCorruptFiles() throws Exception {
    Path basePath = new Path("/projects/testProjectWithCorruptFiles");
    setupTest(basePath, true);
    // Cold run
    new ProjectJob(basePath.toString(), "testProjectWithCorruptFiles", "ns0", "ns1", "ns0",
        routerAdminAddress, 8, 6, 100, 10, false, 0, 0, true, routerContext.getConf()).execute();
    // Hot run
    new ProjectJob(basePath.toString(), "testProjectWithCorruptFiles", "ns0", "ns1", "ns0",
        routerAdminAddress, 8, 6, 0, 10, true, 0, 0, true, routerContext.getConf()).execute();
    assertEquals(23, nnFs1.getContentSummary(basePath).getFileCount());
    GenericTestUtils.LogCapturer logs =
        GenericTestUtils.LogCapturer.captureLogs(LoggerFactory.getLogger(AnalyzeJob.class));
    new ProjectJob(basePath.toString(), "testProjectWithCorruptFiles", "ns0", "ns1", "ns0",
        routerAdminAddress, 8, 6, 100, 10, false, 0, 0, true, routerContext.getConf()).execute();
    assertTrue(logs.getOutput().contains("Ignoring path with corrupt file /projects/testProjectWithCorruptFiles/0"));
    assertTrue(logs.getOutput().contains("Ignoring path with corrupt file /projects/testProjectWithCorruptFiles/2/2"));
    assertFalse(logs.getOutput().contains("Ignoring path with corrupt file /projects/testProjectWithCorruptFiles/0/0"));
  }

  private static void setupTest(Path basePath, boolean corruptFile) throws Exception {
    boolean corruptColdOnce = false;
    boolean corruptHotOnce = false;
    long oldTime = Time.now() - 86400 * 1000 * 20;
    for (int i = 0; i < 5; i++) {
      Path outerPath = new Path(basePath, String.valueOf(i));
      for (int j = 0; j < 5; j++) {
        Path dirPath = new Path(outerPath, String.valueOf(j));
        Path filePath = new Path(dirPath, "file");
        nnFs0.mkdirs(dirPath, new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));
        try (FSDataOutputStream os = nnFs0.create(filePath, (short) 1)) {
          os.writeChars("test");
        }
        nnFs0.setOwner(filePath, "alice", "bob");
        // First 2 dirs are fully cold, next 2 dirs are partially cold
        if (i < 2 || i < 4 && j < 2) {
          if (!corruptColdOnce && corruptFile) {
            corruptColdOnce = true;
            corruptBlock(filePath);
          }
          nnFs0.setTimes(dirPath, oldTime, -1);
          nnFs0.setTimes(filePath, oldTime, -1);
        } else if (!corruptHotOnce && corruptFile) {
          corruptHotOnce = true;
          corruptBlock(filePath);
        }
      }
    }
    if (corruptFile) {
      GenericTestUtils.waitFor(() -> {
        try {
          for (DataNode dn : cluster.getCluster().getDataNodes()) {
            DataNodeTestUtils.runDirectoryScanner(dn);
          }
          List<Path> corruptFiles = new ArrayList<>();
          RemoteIterator<Path> ite = nnFs0.listCorruptFileBlocks(basePath);
          while (ite.hasNext()) {
            corruptFiles.add(ite.next());
          }
          return corruptFiles.size() == 2;
        } catch (IOException e) {
          return true;
        }
      }, 1000, 10000);
    }
  }

  private static void corruptBlock(Path filePath) throws Exception {
    ExtendedBlock block = DFSTestUtil.getFirstBlock(nnFs0, filePath);
    for (int i = 0; i < 3; i++) {
      try {
        cluster.getCluster().deleteMeta(i, block);
      } catch (Exception ignored) {
      }
    }
  }
}
