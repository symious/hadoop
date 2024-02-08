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
package org.apache.hadoop.hdfs.tools;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestVerifyReadable {
  private Configuration conf = new Configuration();
  private MiniDFSCluster cluster;
  private DebugAdmin admin;
  private static final short REPLICATION = 2;
  private static final long BLOCK_SIZE = 1024;

  @Before
  public void setUp() throws Exception {
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, REPLICATION);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    // Faster timeout for testing
    conf.setInt(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 1);
    conf.setLong(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_RETRY_INTERVAL_KEY, 300);
    admin = new DebugAdmin(conf);
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  private int runDebugCommand(Path path) {
    return admin.run(new String[] { "verifyReadable", "-path", path.toString() });
  }

  @Test
  public void testReadable() throws Exception {
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();
    DistributedFileSystem fs = cluster.getFileSystem();

    // Successful case with various block lengths and replications
    {
      Path testPath = new Path("/testReadable1Repl.txt");
      DFSTestUtil.createFile(fs, testPath, BLOCK_SIZE, (short) 1, 1234);
      Assert.assertEquals(0, runDebugCommand(testPath));

      testPath = new Path("/testReadable3Repl.txt");
      DFSTestUtil.createFile(fs, testPath, BLOCK_SIZE, (short) 3, 1234);
      Assert.assertEquals(0, runDebugCommand(testPath));

      testPath = new Path("/testReadableLong3Repl.txt");
      DFSTestUtil.createFile(fs, testPath, BLOCK_SIZE * 16, (short) 3, 1234);
      Assert.assertEquals(0, runDebugCommand(testPath));
    }

    // Simple failure cases
    {
      // File not found
      Path testPath = new Path("/test404.txt");
      Assert.assertEquals(1, runDebugCommand(testPath));
    }

    // Missing replicas
    {
      // Deleted replica
      Path testPath = new Path("/testMissingBlocks.txt");
      DFSTestUtil.createFile(fs, testPath, BLOCK_SIZE * 3, (short) 2, 1234);
      // Still readable with 1 replica left
      deleteReplica(fs, testPath, 1, 1);
      Assert.assertEquals(0, runDebugCommand(testPath));
      // Unreadable when all replicas are gone for a block
      deleteReplica(fs, testPath, 1, 0);
      Assert.assertEquals(1, runDebugCommand(testPath));
    }

    // Down DNs
    {
      Path testPath = new Path("/testMissingDNs.txt");
      DFSTestUtil.createFile(fs, testPath, BLOCK_SIZE * 3, (short) 2, 1234);
      // Still readable with 1 replica left
      shutdownDn(fs, testPath, 1, 1);
      Assert.assertEquals(0, runDebugCommand(testPath));
      // Unreadable when all replicas are gone for a block
      shutdownDn(fs, testPath, 1, 0);
      Assert.assertEquals(1, runDebugCommand(testPath));
    }
  }

  private void deleteReplica(FileSystem fs, Path path, int blkIdx, int dnIndex) throws IOException {
    HdfsLocatedFileStatus locs = (HdfsLocatedFileStatus) fs.listFiles(path, true).next();
    String dnToInvalidate =
        locs.getLocatedBlocks().get(blkIdx).getLocations()[dnIndex].getDatanodeUuid();
    DataNode matchedDn = null;
    for (DataNode dn : cluster.getDataNodes()) {
      if (dn.getDatanodeUuid().equals(dnToInvalidate)) {
        matchedDn = dn;
        break;
      }
    }
    FsDatasetSpi fsdataset = matchedDn.getFSDataset();
    fsdataset.invalidate(cluster.getNamesystem().getBlockPoolId(),
        new Block[] { locs.getLocatedBlocks().get(blkIdx).getBlock().getLocalBlock() });
  }

  private void shutdownDn(FileSystem fs, Path path, int blkIdx, int dnIndex) throws IOException {
    HdfsLocatedFileStatus locs = (HdfsLocatedFileStatus) fs.listFiles(path, true).next();
    String dnToShutdown =
        locs.getLocatedBlocks().get(blkIdx).getLocations()[dnIndex].getDatanodeUuid();
    int idx = 0;
    for (DataNode dn : cluster.getDataNodes()) {
      if (dn.getDatanodeUuid().equals(dnToShutdown)) {
        cluster.shutdownDataNode(idx);
        break;
      }
      idx++;
    }
  }
}
