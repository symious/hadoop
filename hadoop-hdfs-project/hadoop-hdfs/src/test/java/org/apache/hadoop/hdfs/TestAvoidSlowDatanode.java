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
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_AVOID_SLOW_DATANODES_FOR_READ_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_CONTEXT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_SLOW_NODE_CACHE_EXPIRY_MS_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_SLOW_NODE_CACHE_SIZE_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_SLOW_NODE_CACHE_THRESHOLD_MS_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;

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
      } catch (BlockMissingException e) {
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

    DFSInputStream in =
        dfsClient.open("/testSlowNodeDetectionNonHedgedRead");

    assertFalse(dfsClient.isHedgedReadsEnabled());
    assertEquals(0,
        dfsClient.getClientContext().getSlowNodeCache().size());

    try {
      try {
        in.read(0, new byte[256], 0, 256);
      } catch (BlockMissingException e) {
      }

      assertEquals(1,
          dfsClient.getClientContext().getSlowNodeCache().size());

    } finally {
      in.close();
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
      } catch (BlockMissingException e) {
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

    DFSInputStream in1 =
        dfsClient1.open("/testTwoClientSlowNodeDetectionNonHedgedRead");
    DFSInputStream in2 =
        dfsClient2.open("/testTwoClientSlowNodeDetectionNonHedgedRead");

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

    try {
      try {
        in1.read(0, new byte[256], 0, 256);
      } catch (BlockMissingException e) {
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
      Mockito.reset(dfsClient1);
      Mockito.reset(dfsClient2);
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
      out.close();
    }
  }

  private void deleteFile(FileSystem fs, Path filePath) throws IOException {
    fs.delete(filePath, true);
  }
}
