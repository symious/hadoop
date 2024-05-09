/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.util.Timer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Ignore
public class TestDataTransferThrottling {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestDataTransferThrottling.class);
  private static MiniDFSCluster cluster;
  private static FileSystem fs;
  private static final Timer timer = new Timer();

  @BeforeClass
  public static void initialize() throws IOException {
    int numDataNodes = 1;

    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, numDataNodes);
    cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(numDataNodes).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
  }

  @AfterClass
  public static void cleanup() {
    cluster.shutdown();
  }

  void createFile(Path path, int fileLen) throws IOException {
    if (fs.exists(path)) {
      fs.delete(path, true);
    }
    byte[] arr = new byte[fileLen];
    FSDataOutputStream out = fs.create(path);
    out.write(arr);
    out.close();
  }

  void readFile(Path path, int fileLen) throws IOException {
    byte[] arr = new byte[fileLen];
    FSDataInputStream in = fs.open(path);
    in.readFully(arr);
  }

  @Test(timeout = 10000)
  public void testThrottledReads() throws IOException {
    Path file = new Path("throttleRead.test");

    int fileLen = 4096;
    createFile(file, fileLen);

    refreshDNThrottlerThenTestOp(9000, file, fileLen, true);
    refreshDNThrottlerThenTestOp(4096, file, fileLen, true);
    refreshDNThrottlerThenTestOp(1024, file, fileLen, true);
  }

  @Test(timeout = 10000)
  public void testThrottledWrites() throws IOException {
    Path file = new Path("throttleWrite.test");

    int fileLen = 4096;
    createFile(file, fileLen);

    refreshDNThrottlerThenTestOp(9000, file, fileLen, false);
    refreshDNThrottlerThenTestOp(4096, file, fileLen, false);
    refreshDNThrottlerThenTestOp(1024, file, fileLen, false);
  }

  @Test(timeout = 20000)
  public void testThrottledParallelOps() throws IOException {
    testThrottledParallelOps(true);
    testThrottledParallelOps(false);
  }

  private void testThrottledParallelOps(final boolean isRead)
      throws IOException {
    final int MAX_ATTEMPTS = 5;
    final int nThreads = 4;
    final int fileLen = 100000;
    final int bandwidth = 200000;

    if (isRead) {
      createFile(new Path("throttleOp.test"), fileLen);
    }
    refreshDNThrottler(bandwidth, isRead);

    for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
      try {
        Thread[] threads = new Thread[nThreads];
        for (int i = 0; i < nThreads; i++) {
          final int finalI = i;
          threads[i] = new Thread(new Runnable() {
            @Override
            public void run() {
              try {
                if (isRead) {
                  readFile(new Path("throttleOp.test"), fileLen);
                } else {
                  createFile(new Path("throttleOp" + finalI + ".test"),
                      fileLen);
                }
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            }
          });
        }

        long startRead = timer.monotonicNow();

        for (int i = 0; i < nThreads; i++) {
          threads[i].start();
        }
        for (int i = 0; i < nThreads; i++) {
          threads[i].join();
        }

        long opDuration = timer.monotonicNow() - startRead;

        LOG.info(
            "Op took {}ms for fileLen {} with bandwidth {} on {} parallel ops",
            opDuration, fileLen, bandwidth, nThreads);
        // Expected op time is around 2 seconds
        assertApproximateTime(opDuration, 2000);
        // Break loop if test passes
        return;
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } catch (AssertionError e) {
        LOG.info("Throttled DN parallel ops test attempt {}/{} failed.",
            attempt + 1, MAX_ATTEMPTS);
      }
    }
    fail("All throttled DN parallel ops tests failed.");
  }

  private void refreshDNThrottler(long bandwidth, boolean isRead)
      throws IOException {
    Configuration conf = new HdfsConfiguration();
    if (isRead) {
      conf.setLong(DFSConfigKeys.DFS_DATANODE_DATA_READ_BANDWIDTHPERSEC_KEY,
          bandwidth);
    } else {
      conf.setLong(DFSConfigKeys.DFS_DATANODE_DATA_WRITE_BANDWIDTHPERSEC_KEY,
          bandwidth);
    }

    DataNode datanode = cluster.getDataNodes().get(0);
    datanode.refreshThrottlerConfig(conf);
  }

  private void refreshDNThrottlerThenTestOp(long bandwidth, Path file,
      int fileLen, boolean isRead) throws IOException {
    refreshDNThrottler(bandwidth, isRead);
    int expectedReadTimeMs =
        (int) Math.floor((float) fileLen / bandwidth) * 1000;
    assertOpTimeUnder(expectedReadTimeMs, bandwidth, file, fileLen, isRead);
  }

  private void assertOpTimeUnder(long threshold, long bandwidth, Path file,
      int fileLen, boolean isRead) throws IOException {
    long startRead = timer.monotonicNow();
    if (isRead) {
      readFile(file, fileLen);
    } else {
      createFile(file, fileLen);
    }
    long opDuration = timer.monotonicNow() - startRead;
    LOG.info("Op took {}ms for fileLen {} with bandwidth {}", opDuration,
        fileLen, bandwidth);
    assertApproximateTime(opDuration, threshold);
  }

  private void assertApproximateTime(long testTime, long target) {
    float ratio = (float) testTime / target;
    // If the op is fast enough that target time is 0 seconds, 100ms should suffice
    if (target == 0) {
      assertTrue(testTime < 100);
    } else {
      assertTrue(0.95 < ratio && ratio < 1.05);
    }
  }
}
