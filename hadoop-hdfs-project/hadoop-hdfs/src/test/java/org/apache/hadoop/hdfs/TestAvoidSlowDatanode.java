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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.event.Level;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_AVOID_SLOW_DATANODES_FOR_READ_EC_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_AVOID_SLOW_DATANODES_FOR_READ_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_CONTEXT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_SLOW_NODE_CACHE_EXPIRY_MS_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_SLOW_NODE_CACHE_SIZE_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_SLOW_NODE_CACHE_THRESHOLD_MS_KEY;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Tests for dead node detection in DFSClient.
 */
public class TestAvoidSlowDatanode {

  private MiniDFSCluster cluster;
  private Configuration conf;

  @Before
  public void setUp() {
    cluster = null;
    conf = new HdfsConfiguration();
    conf.setBoolean(DFS_CLIENT_AVOID_SLOW_DATANODES_FOR_READ_KEY, true);
    conf.setInt(DFS_CLIENT_SLOW_NODE_CACHE_EXPIRY_MS_KEY, 2000);
    conf.setInt(DFS_CLIENT_SLOW_NODE_CACHE_SIZE_KEY, 10);
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test(timeout = 30000)
  public void testSlowNodeDetectionHedgedRead() throws Exception {
    final int slownodeThreshold = 100;

    conf.set(DFS_CLIENT_CONTEXT, "testSlowNodeDetectionHedgedRead");
    conf.setInt(HdfsClientConfigKeys.HedgedRead.THREADPOOL_SIZE_KEY, 2);
    conf.setLong(HdfsClientConfigKeys.HedgedRead.THRESHOLD_MILLIS_KEY,
        slownodeThreshold);

    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3)
        .format(true)
        .build();
    cluster.waitActive();

    FileSystem fs = cluster.getFileSystem();
    Path filePath = new Path("/testSlowNodeDetectionHedgedRead");
    createFile(fs, filePath);

    DFSClientFaultInjector oldFaultInjector = DFSClientFaultInjector.get();
    DFSClientFaultInjector.set(new DFSClientFaultInjector(){
      @Override
      public void sleepBeforeHedgedGet() {
        try {
          Thread.sleep(2 * slownodeThreshold);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    });

    FSDataInputStream in = fs.open(filePath);
    DFSInputStream din = (DFSInputStream) in.getWrappedStream();
    DFSClient dfsClient = din.getDFSClient();

    assertEquals(0,
        dfsClient.getClientContext().getSlowNodeCache().size());

    try {
      try {
        in.read(0, new byte[256], 0, 256);
      } catch (BlockMissingException ignored) {
      }

      assertEquals(1,
          dfsClient.getClientContext().getSlowNodeCache().size());

    } finally {
      in.close();
      deleteFile(fs, filePath);
      DFSClientFaultInjector.set(oldFaultInjector);
    }
  }

  @Test(timeout = 30000)
  public void testSlowNodeDetectionNonHedgedRead() throws Exception {
    final long slownodeThreshold = 100;

    conf.set(DFS_CLIENT_CONTEXT, "testSlowNodeDetectionNonHedgedRead");
    conf.setLong(DFS_CLIENT_SLOW_NODE_CACHE_THRESHOLD_MS_KEY,
        slownodeThreshold);

    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3)
        .format(true)
        .build();
    cluster.waitActive();

    DistributedFileSystem fs = cluster.getFileSystem();
    DFSClient dfsClient = Mockito.spy(fs.getClient());
    Mockito.when(dfsClient.isHedgedReadsEnabled()).thenReturn(false);

    Path filePath = new Path("/testSlowNodeDetectionNonHedgedRead");
    createFile(fs, filePath);

    DFSClientFaultInjector oldFaultInjector = DFSClientFaultInjector.get();
    DFSClientFaultInjector.set(new DFSClientFaultInjector() {
      @Override
      public void readFromDatanodeDelay() {
        try {
          Thread.sleep(2 * slownodeThreshold);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    });

    try (DFSInputStream in = dfsClient.open(
        "/testSlowNodeDetectionNonHedgedRead")) {
      assertFalse(dfsClient.isHedgedReadsEnabled());
      assertEquals(0,
          dfsClient.getClientContext().getSlowNodeCache().size());
      try {
        in.read(0, new byte[256], 0, 256);
      } catch (BlockMissingException ignored) {
      }

      assertEquals(1,
          dfsClient.getClientContext().getSlowNodeCache().size());

    } finally {
      deleteFile(fs, filePath);
      Mockito.reset(dfsClient);
      DFSClientFaultInjector.set(oldFaultInjector);
    }
  }

  @Test
  public void testSlowNodeIsAvoided() throws Exception {
    conf.set(DFS_CLIENT_CONTEXT, "testSlowNodeIsAvoided");

    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3)
        .format(true)
        .build();
    cluster.waitActive();

    FileSystem fs = cluster.getFileSystem();
    Path filePath = new Path("/testSlowNodeIsAvoided");
    createFile(fs, filePath);

    FSDataInputStream in = fs.open(filePath);
    DFSInputStream din = (DFSInputStream) in.getWrappedStream();
    DFSClient dfsClient = din.getDFSClient();

    try {
      List<LocatedBlock> blocks = DFSTestUtil.getAllBlocks(in);
      LocatedBlock block = blocks.get(0);
      DatanodeInfo slowDatanode = block.getLocations()[0];

      dfsClient.addSlowNode(slowDatanode);

      din.avoidSlowDatanodes(block);
      assertEquals(slowDatanode, block.getLocations()[2]);
    } finally {
      in.close();
      deleteFile(fs, filePath);
    }
  }

  @Test
  public void testDataNodesCacheMetrics() throws Exception {
    conf.set(DFS_CLIENT_CONTEXT, "testSlowNodeIsAvoided");

    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3)
        .format(true)
        .build();
    cluster.waitActive();

    FileSystem fs = cluster.getFileSystem();
    Path filePath = new Path("/testSlowNodeIsAvoided");
    createFile(fs, filePath);

    FSDataInputStream in = fs.open(filePath);
    DFSInputStream din = (DFSInputStream) in.getWrappedStream();
    DFSClient dfsClient = din.getDFSClient();
    DFSSlowDatanodeCacheMetrics metrics =
        dfsClient.getSlowDatanodeCacheMetricsMetric();

    try {
      List<LocatedBlock> blocks = DFSTestUtil.getAllBlocks(in);
      LocatedBlock block = blocks.get(0);
      DatanodeInfo slowDatanode = block.getLocations()[0];

      long sortOpsBefore = metrics.getSortingOps();
      long sortingOpsWinsBefore = metrics.getSortingOpsWins();

      dfsClient.addSlowNode(slowDatanode);

      din.avoidSlowDatanodes(block);
      assertEquals(slowDatanode, block.getLocations()[2]);
      assertEquals(1, metrics.getSortingOps() - sortOpsBefore);
      assertEquals(1,
          metrics.getSortingOpsWins() - sortingOpsWinsBefore);
    } finally {
      in.close();
    }

    // All nodes are slow hence not efficient sorting.
    FSDataInputStream in2 = fs.open(filePath);
    DFSInputStream din2 = (DFSInputStream) in2.getWrappedStream();
    DFSClient dfsClient2 = din2.getDFSClient();

    try {
      List<LocatedBlock> blocks = DFSTestUtil.getAllBlocks(in);
      LocatedBlock block = blocks.get(0);

      for (DatanodeInfo datanode : block.getLocations()) {
        dfsClient2.addSlowNode(datanode);
      }

      long sortOpsBefore = metrics.getSortingOps();
      long sortingOpsWinsBefore = metrics.getSortingOpsWins();

      din.avoidSlowDatanodes(block);
      assertEquals(1,metrics.getSortingOps() - sortOpsBefore);
      assertEquals(0,
          metrics.getSortingOpsWins() - sortingOpsWinsBefore);
    } finally {
      in2.close();
      deleteFile(fs, filePath);
    }
  }

  @Test(timeout = 30000)
  public void testTwoClientSlowNodeDetectionHedgedRead() throws Exception {
    final int slownodeThreshold = 100;

    conf.set(DFS_CLIENT_CONTEXT, "testTwoClientSlowNodeDetectionHedgedRead");
    conf.setInt(HdfsClientConfigKeys.HedgedRead.THREADPOOL_SIZE_KEY, 2);
    conf.setLong(HdfsClientConfigKeys.HedgedRead.THRESHOLD_MILLIS_KEY,
        slownodeThreshold);

    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3)
        .format(true)
        .build();
    cluster.waitActive();

    FileSystem fs = cluster.getFileSystem();
    Path filePath = new Path("/testTwoClientSlowNodeDetectionHedgedRead");
    createFile(fs, filePath);

    DFSClientFaultInjector oldFaultInjector = DFSClientFaultInjector.get();
    DFSClientFaultInjector.set(new DFSClientFaultInjector(){
      @Override
      public void sleepBeforeHedgedGet() {
        try {
          Thread.sleep(2 * slownodeThreshold);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    });

    FSDataInputStream in1 = fs.open(filePath);
    DFSInputStream din1 = (DFSInputStream) in1.getWrappedStream();
    DFSClient dfsClient1 = din1.getDFSClient();

    FSDataInputStream in2 = fs.open(filePath);
    DFSInputStream din2 = (DFSInputStream) in2.getWrappedStream();
    DFSClient dfsClient2 = din2.getDFSClient();

    // dfsClient1 and dfsClient2 should share slowNodeCache
    assertSame(dfsClient1.getClientContext().getSlowNodeCache(),
        dfsClient2.getClientContext().getSlowNodeCache());

    // slowNodeCache should have size 0 with zero slow read
    assertEquals(0,
        dfsClient1.getClientContext().getSlowNodeCache().size());
    assertEquals(0,
        dfsClient2.getClientContext().getSlowNodeCache().size());

    try {
      try {
        in1.read(0, new byte[256], 0, 256);
      } catch (BlockMissingException ignored) {
      }

      // dfsClient1 and dfsClient2 should share slowNodeCache
      assertSame(dfsClient1.getClientContext().getSlowNodeCache(),
          dfsClient2.getClientContext().getSlowNodeCache());

      // slowNodeCache should have size 1 with one slow read
      assertEquals(1,
          dfsClient1.getClientContext().getSlowNodeCache().size());
      assertEquals(1,
          dfsClient2.getClientContext().getSlowNodeCache().size());

    } finally {
      in1.close();
      in2.close();
      deleteFile(fs, filePath);
      DFSClientFaultInjector.set(oldFaultInjector);
    }
  }

  @Test(timeout = 30000)
  public void testTwoClientSlowNodeDetectionNonHedgedRead() throws Exception {
    final long slownodeThreshold = 100;

    conf.set(DFS_CLIENT_CONTEXT, "testTwoClientSlowNodeDetectionNonHedgedRead");
    conf.setLong(DFS_CLIENT_SLOW_NODE_CACHE_THRESHOLD_MS_KEY,
        slownodeThreshold);

    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3)
        .format(true)
        .build();
    cluster.waitActive();

    DistributedFileSystem fs = cluster.getFileSystem();

    DFSClient dfsClient1 = Mockito.spy(fs.getClient());
    Mockito.when(dfsClient1.isHedgedReadsEnabled()).thenReturn(false);
    DFSClient dfsClient2 = Mockito.spy(fs.getClient());
    Mockito.when(dfsClient2.isHedgedReadsEnabled()).thenReturn(false);

    Path filePath = new Path("/testTwoClientSlowNodeDetectionNonHedgedRead");
    createFile(fs, filePath);

    DFSClientFaultInjector oldFaultInjector = DFSClientFaultInjector.get();
    DFSClientFaultInjector.set(new DFSClientFaultInjector() {
      @Override
      public void readFromDatanodeDelay() {
        try {
          Thread.sleep(2 * slownodeThreshold);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    });

    assertFalse(dfsClient1.isHedgedReadsEnabled());
    assertFalse(dfsClient2.isHedgedReadsEnabled());

    // dfsClient1 and dfsClient2 should share slowNodeCache
    assertSame(dfsClient1.getClientContext().getSlowNodeCache(),
        dfsClient2.getClientContext().getSlowNodeCache());

    // slowNodeCache should have size 0 with zero slow read
    assertEquals(0,
        dfsClient1.getClientContext().getSlowNodeCache().size());
    assertEquals(0,
        dfsClient2.getClientContext().getSlowNodeCache().size());

    try (DFSInputStream in1 = dfsClient1.open(
        "/testTwoClientSlowNodeDetectionNonHedgedRead");
         DFSInputStream ignored1 = dfsClient2.open(
             "/testTwoClientSlowNodeDetectionNonHedgedRead")) {
      try {
        in1.read(0, new byte[256], 0, 256);
      } catch (BlockMissingException ignored) {
      }

      // dfsClient1 and dfsClient2 should share slowNodeCache
      assertSame(dfsClient1.getClientContext().getSlowNodeCache(),
          dfsClient2.getClientContext().getSlowNodeCache());

      // slowNodeCache should have size 1 with one slow read
      assertEquals(1,
          dfsClient1.getClientContext().getSlowNodeCache().size());
      assertEquals(1,
          dfsClient2.getClientContext().getSlowNodeCache().size());

    } finally {
      deleteFile(fs, filePath);
      Mockito.reset(dfsClient1);
      Mockito.reset(dfsClient2);
      DFSClientFaultInjector.set(oldFaultInjector);
    }
  }

  @Test(timeout = 50000)
  public void testSlowNodeWhenReadEC() throws Exception {

    GenericTestUtils.setLogLevel(DFSClient.LOG, Level.DEBUG);
    GenericTestUtils.LogCapturer logs =
        GenericTestUtils.LogCapturer.captureLogs(DFSClient.LOG);

    Configuration conf = new HdfsConfiguration();
    // Set slow node cache parameter.
    final long slowNodeThreshold = 100;
    final int slowNodeCacheExpiryMillis = 2000;
    conf.set(DFS_CLIENT_CONTEXT, "testSlowNodeWhenReadEC");
    conf.setLong(DFS_CLIENT_SLOW_NODE_CACHE_THRESHOLD_MS_KEY,
        slowNodeThreshold);
    conf.setBoolean(DFS_CLIENT_AVOID_SLOW_DATANODES_FOR_READ_EC_KEY, true);
    conf.setInt(DFS_CLIENT_SLOW_NODE_CACHE_EXPIRY_MS_KEY, slowNodeCacheExpiryMillis);
    conf.setInt(DFS_CLIENT_SLOW_NODE_CACHE_SIZE_KEY, 10);

    ErasureCodingPolicy ecPolicy = StripedFileTestUtil.getDefaultECPolicy();
    short dataBlocks = (short) ecPolicy.getNumDataUnits();
    short parityBlocks = (short) ecPolicy.getNumParityUnits();
    int cellSize = ecPolicy.getCellSize();
    int stripesPerBlock = 2;
    int blockSize = stripesPerBlock * cellSize;

    DFSClientFaultInjector oldFaultInjector = DFSClientFaultInjector.get();

    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).
        numDataNodes(dataBlocks + parityBlocks + 2).format(true).build()) {

      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();

      // Create ec file.
      fs.enableErasureCodingPolicy(ecPolicy.getName());
      fs.mkdirs(new Path("/ec"));
      cluster.getFileSystem().getClient().setErasureCodingPolicy("/ec",
          ecPolicy.getName());
      int dataSize = 1024 * 1024 * 10;
      byte[] expected = StripedFileTestUtil.generateBytes(dataSize);
      Path src = new Path("/ec/file");
      DFSTestUtil.writeFile(fs, src, new String(expected));
      StripedFileTestUtil.waitBlockGroupsReported(fs, src.toString());
      StripedFileTestUtil.verifyLength(fs, src, dataSize);
      LocatedBlocks blks = fs.getClient().getLocatedBlocks(src.toString(), 0);
      LocatedStripedBlock block = (LocatedStripedBlock) blks.getLastLocatedBlock();
      DatanodeInfo slowDn = block.getLocations()[0];
      DFSClient dfsClient = fs.getClient();
      ClientContext clientContext = dfsClient.getClientContext();
      // Mock the datanode is slow node.
      DFSClientFaultInjector.set(new DFSClientFaultInjector() {
        @Override
        public void readECFromDatanodeDelay(DatanodeInfo datanode) {
          try {
            if (datanode.equals(slowDn)) {
              Thread.sleep(2 * slowNodeThreshold);
            }
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      });

      assertEquals(0, clientContext.getSlowNodeCache().size());

      // Validate read ec file.
      // Slow node appears when reading, read next stripe will skip slow node.
      int done = 0;
      ByteBuffer readBuffer = ByteBuffer.allocate(dataSize);
      try (DFSInputStream in = dfsClient.open("/ec/file")) {
        while (done < dataSize) {
          int ret = in.read(readBuffer);
          assertTrue(ret > 0);
          done += ret;
        }
        assertArrayEquals(expected, readBuffer.array());
        assertEquals(1, clientContext.getSlowNodeCache().size());
        assertTrue(clientContext.getSlowNodeCache().isSlowNode(slowDn));
        assertTrue(logs.getOutput().contains("Slow node " + slowDn.getXferAddr() +
            " will skip read."));
        // Validate run ec decoding for read data.
        assertTrue(in.getReadStatistics().getTotalEcDecodingTimeMillis() > 0);
        logs.clearOutput();
      }

      // Slow node appears before reading, read data from slow node
      // will skip create block reader.
      done = 0;
      readBuffer = ByteBuffer.allocate(dataSize);
      try (DFSInputStream in = dfsClient.open("/ec/file")) {
        while (done < dataSize) {
          int ret = in.read(readBuffer);
          assertTrue(ret > 0);
          done += ret;
        }
        assertArrayEquals(expected, readBuffer.array());
        assertTrue(clientContext.getSlowNodeCache().isSlowNode(slowDn));
        assertTrue(logs.getOutput().contains("Slow node " + slowDn.getXferAddr() +
            " will skip create block reader."));
        // Validate run ec decoding for read data.
        assertTrue(in.getReadStatistics().getTotalEcDecodingTimeMillis() > 0);
        logs.clearOutput();
      }

      // Wait slow node cache expire.
      GenericTestUtils.waitFor(()
              -> !clientContext.getSlowNodeCache().isSlowNode(slowDn),
          10, 2000);

      // Validate pread ec file.
      try (DFSInputStream in = dfsClient.open("/ec/file")) {
        byte[] buf = new byte[dataSize];
        in.read(0, buf, 0, dataSize);
        assertArrayEquals(buf, expected);
        assertTrue(clientContext.getSlowNodeCache().isSlowNode(slowDn));
        assertTrue(logs.getOutput().contains("Slow node " + slowDn.getXferAddr() +
            " will skip read."));
        // Validate run ec decoding for read data.
        assertTrue(in.getReadStatistics().getTotalEcDecodingTimeMillis() > 0);
        logs.clearOutput();
      }

      // Wait slow node cache expire.
      GenericTestUtils.waitFor(()
              -> !clientContext.getSlowNodeCache().isSlowNode(slowDn),
          10, 2000);

      // Validate read ec file when turn-off `avoidSlowDataNodesForReadEC`.
      clientContext.setAvoidSlowDataNodesForReadEC(false);
      done = 0;
      readBuffer = ByteBuffer.allocate(dataSize);
      try (DFSInputStream in = dfsClient.open("/ec/file")) {
        while (done < dataSize) {
          int ret = in.read(readBuffer);
          assertTrue(ret > 0);
          done += ret;
        }
        assertArrayEquals(expected, readBuffer.array());
        // Slow node logic will not take effect.
        assertFalse(clientContext.getSlowNodeCache().isSlowNode(slowDn));
        assertFalse(logs.getOutput().contains("Slow node " + slowDn.getXferAddr() +
            " will skip read."));
        // Validate not run ec decoding for read data.
        assertEquals(0, in.getReadStatistics().getTotalEcDecodingTimeMillis());
        logs.clearOutput();
      }
    } finally {
      DFSClientFaultInjector.set(oldFaultInjector);
    }
  }

  private void createFile(FileSystem fs, Path filePath) throws IOException {
    FSDataOutputStream out = null;
    try {
      // 256 bytes data chunk for writes
      byte[] bytes = new byte[256];
      Arrays.fill(bytes, (byte) '0');

      // File with a 512 bytes block size
      out = fs.create(filePath, true, 4096, (short) 3, 512);

      // Write a block to all 3 DNs (2x256bytes).
      out.write(bytes);
      out.write(bytes);
      out.hflush();

    } finally {
      assert out != null;
      out.close();
    }
  }

  private void deleteFile(FileSystem fs, Path filePath) throws IOException {
    fs.delete(filePath, true);
  }
}