package org.apache.hadoop.tools.federation;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.MultipleDestinationMountTableResolver;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.tools.federation.migration.CreateTopDirJob;
import org.apache.hadoop.tools.federation.migration.MigrationJob;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.hadoop.tools.federation.migration.MigrationJob.toggleSkipTopTwoLevelsForTesting;

public class TestNSMigrationToolCreateTopDir {
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
  public void testCreateTopDir() throws IOException {
    nnFs0.mkdirs(new Path("/test"), new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
    nnFs0.setOwner(new Path("/test"), "a", "a");
    nnFs0.mkdirs(new Path("/test/1"),
        new FsPermission(FsAction.ALL, FsAction.WRITE_EXECUTE, FsAction.ALL));
    nnFs0.setOwner(new Path("/test/1"), "b", "b");
    nnFs0.mkdirs(new Path("/test/1/2"),
        new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.NONE));
    nnFs0.mkdirs(new Path("/test/1/3"),
        new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.NONE));
    nnFs0.setOwner(new Path("/test/1/2"), "c", "c");
    nnFs0.setOwner(new Path("/test/1/3"), "c", "c");

    File tempInput = File.createTempFile("testCreateTopDir", ".txt");
    try (BufferedWriter bw = new BufferedWriter(new FileWriter(tempInput))) {
      bw.write("/test/1/2\n");
      bw.write("/test/1/3");
    }

    CreateTopDirJob job = new CreateTopDirJob("/test", tempInput.getAbsolutePath(), "ns0", "ns1",
        routerContext.getConf());
    job.execute();

    FileStatus statuss0 = nnFs1.getFileStatus(new Path("/test"));
    FileStatus statuss1 = nnFs1.getFileStatus(new Path("/test/1"));
    FileStatus statuss2 = nnFs1.getFileStatus(new Path("/test/1/2"));
    FileStatus statuss3 = nnFs1.getFileStatus(new Path("/test/1/3"));
    FileStatus statusd0 = nnFs1.getFileStatus(new Path("/test"));
    FileStatus statusd1 = nnFs1.getFileStatus(new Path("/test/1"));
    FileStatus statusd2 = nnFs1.getFileStatus(new Path("/test/1/2"));
    FileStatus statusd3 = nnFs1.getFileStatus(new Path("/test/1/3"));
    Assert.assertEquals(statuss0.getOwner(), statusd0.getOwner());
    Assert.assertEquals(statuss0.getGroup(), statusd0.getGroup());
    Assert.assertEquals(statuss1.getOwner(), statusd1.getOwner());
    Assert.assertEquals(statuss1.getGroup(), statusd1.getGroup());
    Assert.assertEquals(statuss2.getOwner(), statusd2.getOwner());
    Assert.assertEquals(statuss2.getGroup(), statusd2.getGroup());
    Assert.assertEquals(statuss3.getOwner(), statusd3.getOwner());
    Assert.assertEquals(statuss3.getGroup(), statusd3.getGroup());
    Assert.assertEquals(statuss0.getPermission(), statusd0.getPermission());
    Assert.assertEquals(statuss1.getPermission(), statusd1.getPermission());
    Assert.assertEquals(statuss2.getPermission(), statusd2.getPermission());
    Assert.assertEquals(statuss3.getPermission(), statusd3.getPermission());
  }
}
