package org.apache.hadoop.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.util.Timer;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertTrue;

public class TestThrottledCopies {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestThrottledCopies.class);
  private static final int MAX_ATTEMPT = 3;
  private static final int FILE_LEN = 4096000;

  @Test
  public void testThrottledCopies() throws Exception {
    MiniDFSCluster cluster = null;

    try {
      Configuration conf = new Configuration();
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
      final FileSystem fs = cluster.getFileSystem();

      // Write test file
      Path path = new Path("/distcp.test");
      if (fs.exists(path)) {
        fs.delete(path, true);
      }
      byte[] arr = new byte[FILE_LEN];
      FSDataOutputStream out = fs.create(path);
      out.write(arr);
      out.close();

      testThrottledCopy(cluster, conf, path, 2048000);
      testThrottledCopy(cluster, conf, path, 512000);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  private void testThrottledCopy(MiniDFSCluster cluster, Configuration conf,
      Path path, long bandwidth) throws Exception {
    for (int i = 0; i < MAX_ATTEMPT; i++) {
      try {
        testThrottledCopyInternal(cluster, conf, path, bandwidth);
        // Terminate early if test passes
        return ;
      } catch (AssertionError ae) {
        LOG.info("Throttled copy test attempt {}/{} fail", i + 1, MAX_ATTEMPT);
      } catch (Exception e) {
        throw e;
      }
    }
  }

  private void testThrottledCopyInternal(MiniDFSCluster cluster,
      Configuration conf, Path path, long bandwidth) throws Exception {
    Configuration cloneConf = new Configuration(conf);
    cloneConf.setLong(DFSConfigKeys.DFS_DATANODE_DATA_WRITE_BANDWIDTHPERSEC_KEY,
        bandwidth);
    cluster.getDataNodes().get(0).refreshThrottlerConfig(cloneConf);

    Timer timer = new Timer();
    long start = timer.monotonicNow();
    ToolRunner.run(new DistCpV1(cloneConf),
        new String[] { path.toString(), "/out.dat" });
    long duration = timer.monotonicNow() - start;

    LOG.info("DistCp took {}ms under bandwidth {}", duration, bandwidth);
    assertApproximate(duration, FILE_LEN * 1000L / bandwidth);
    FileSystem fs = cluster.getFileSystem();
    fs.delete(new Path("/out.dat"), true);
  }

  private void assertApproximate(long tester, long target) {
    assertApproximate(tester, target, (float) 0.1);
  }

  private void assertApproximate(long tester, long target, float epsilon) {
    float ratio = (float) tester / target;
    assertTrue(String.format(
        "Value %s is outside expected range: target=%s, epsilon=%s", tester,
        target, epsilon), 1 - epsilon < ratio && ratio < 1 + epsilon);
  }
}
