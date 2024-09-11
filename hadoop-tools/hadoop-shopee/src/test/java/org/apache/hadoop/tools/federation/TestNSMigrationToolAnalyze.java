package org.apache.hadoop.tools.federation;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.server.federation.StateStoreDFSCluster;
import org.apache.hadoop.hdfs.server.federation.resolver.MultipleDestinationMountTableResolver;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.tools.federation.migration.AnalyzeJob;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.hadoop.tools.federation.migration.MigrationUtils.loadPathsFromDfs;

public class TestNSMigrationToolAnalyze {
  @Test(timeout = 300000L)
  public void testBottomLevelDirs() throws Exception {
    StateStoreDFSCluster cluster =
        new StateStoreDFSCluster(false, 1, MultipleDestinationMountTableResolver.class);
    Configuration conf = new RouterConfigBuilder().stateStore().heartbeat().admin().rpc().build();
    conf.set(RBFConfigKeys.DFS_ROUTER_MONITOR_NAMENODE, "ns0");
    conf.setBoolean(RBFConfigKeys.MOUNT_TABLE_CACHE_UPDATE, true);
    cluster.addRouterOverrides(conf);
    cluster.startCluster();
    cluster.startRouters();
    cluster.waitClusterUp(true);
    MiniRouterDFSCluster.RouterContext routerContext = cluster.getRandomRouter();
    routerContext.getRouter().getStateStore().stopCacheUpdateService();
    DistributedFileSystem fs = (DistributedFileSystem) routerContext.getFileSystem();

    /**
     * /base/
     * /base/hotdir1/hotfile
     * /base/hotdir2/coldfile
     * /base/hotdir2/hotinnerdir/hotfile
     * /base/hotdir2/coldinnerdir/coldfile
     * /base/colddir1/coldfile1
     * /base/colddir1/coldfile2
     * /base/colddir2/coldinnerdir1/coldfile
     * /base/colddir2/coldinnerdir2/coldfile
     * /base/colddir2/coldfile
     */

    setTimes(fs, "/base/hotdir1/hotfile", true, true);
    setTimes(fs, "/base/hotdir2/coldfile", true, false);
    setTimes(fs, "/base/hotdir2/hotinnerdir/hotfile", true, true);
    setTimes(fs, "/base/hotdir2/coldinnerdir/coldfile", true, false);
    setTimes(fs, "/base/colddir1/coldfile1", true, false);
    setTimes(fs, "/base/colddir1/coldfile2", true, false);
    setTimes(fs, "/base/colddir2/coldinnerdir1/coldfile", true, false);
    setTimes(fs, "/base/colddir2/coldinnerdir2/coldfile", true, false);
    setTimes(fs, "/base/colddir2/coldfile", true, false);

    setTimes(fs, "/base/hotdir2/coldinnerdir/", false, false);
    setTimes(fs, "/base/colddir2/coldinnerdir1/", false, false);
    setTimes(fs, "/base/colddir2/coldinnerdir2/", false, false);
    setTimes(fs, "/base/colddir1", false, false);
    setTimes(fs, "/base/colddir2", false, false);

    Path outputFile = new Path("/tmp/test_output.txt");
    new AnalyzeJob("/base", "ns0", "ns0", "7", outputFile, 2, routerContext.getConf()).execute();
    Set<Path> paths = loadPathsFromDfs(fs, new Path("/base"), outputFile);

    Assert.assertEquals(3, paths.size());
    Assert.assertTrue(paths.contains(new Path("/base/hotdir2/coldinnerdir")));
    Assert.assertTrue(paths.contains(new Path("/base/colddir1")));
    Assert.assertTrue(paths.contains(new Path("/base/colddir2")));

    // Do it again, but this time with an opened file and a dir that changed during the process
    setTimes(fs, "/base/colddir2/newdir/file", true, false);
    setTimes(fs, "/base/colddir2/newdir", false, false);
    final CountDownLatch latch = new CountDownLatch(1);
    Thread thread = new Thread(() -> {
      try {
        AnalyzeJobWithWait job = new AnalyzeJobWithWait("/base", "ns0", "ns0", "7", outputFile, 2,
            routerContext.getConf());
        job.setLock(latch);
        job.execute();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
    thread.start();
    FSDataOutputStream stream = fs.append(new Path("/base/colddir2/coldfile"));
    fs.mkdirs(new Path("/base/colddir2/newdir/innerdir"));
    fs.rename(new Path("/base/colddir2/newdir/file"), new Path("/base/colddir2/newdir/innerdir/file"));
    FSDataOutputStream stream2 = fs.create(new Path("/base/colddir2/newdir/innerdir/file"));
    latch.countDown();
    thread.join();

    stream.close();
    stream2.close();
    paths = loadPathsFromDfs(fs, new Path("/base"), outputFile);

    Assert.assertEquals(4, paths.size());
    Assert.assertTrue(paths.contains(new Path("/base/hotdir2/coldinnerdir")));
    Assert.assertTrue(paths.contains(new Path("/base/colddir1")));
    Assert.assertTrue(paths.contains(new Path("/base/colddir2/coldinnerdir1/")));
    Assert.assertTrue(paths.contains(new Path("/base/colddir2/coldinnerdir2/")));
  }

  static class AnalyzeJobWithWait extends AnalyzeJob {

    private CountDownLatch latch;

    public AnalyzeJobWithWait(String path, String srcNs, String fedNs, String threshold,
        Path output, int concurrency, Configuration conf) throws IOException {
      super(path, srcNs, fedNs, threshold, output, concurrency, conf);
    }

    void setLock(CountDownLatch latch) {
      this.latch = latch;
    }

    @Override
    public void waitForTesting() {
      try {
        this.latch.await();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void setTimes(DistributedFileSystem fileSystem, String pathStr, boolean createFile,
      boolean hot) throws IOException {
    Path path = new Path(pathStr);
    if (createFile) {
      fileSystem.mkdirs(path.getParent());
      fileSystem.create(path, true).close();
    }
    if (!hot) {
      long time = Time.now() - 3000000000L;
      fileSystem.setTimes(path, time, -1);
    }
  }
}
