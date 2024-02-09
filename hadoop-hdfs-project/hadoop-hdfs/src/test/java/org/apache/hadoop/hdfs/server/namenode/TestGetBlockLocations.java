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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.net.DFSNetworkTopology;
import org.apache.hadoop.hdfs.net.DFSNetworkTopologyWithDataCenter;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyWithDataCenter;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;
import org.apache.hadoop.hdfs.server.namenode.NameNode.OperationCategory;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.net.StaticMapping;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_CALLER_CONTEXT_ENABLED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AUDIT_LOG_ADD_BLOCKS_ENABLED;
import static org.apache.hadoop.util.Time.now;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestGetBlockLocations {
  private static final String FILE_NAME = "foo";
  private static final String FILE_PATH = "/" + FILE_NAME;
  private static final long MOCK_INODE_ID = 16386;
  private static final String RESERVED_PATH =
      "/.reserved/.inodes/" + MOCK_INODE_ID;

  @Test(timeout = 30000)
  public void testResolveReservedPath() throws IOException {
    FSNamesystem fsn = setupFileSystem();
    FSEditLog editlog = fsn.getEditLog();
    fsn.getBlockLocations("dummy", RESERVED_PATH, 0, 1024);
    verify(editlog).logTimes(eq(FILE_PATH), anyLong(), anyLong());
    fsn.close();
  }

  @Test(timeout = 30000)
  public void testGetBlockLocationsRacingWithDelete() throws IOException {
    FSNamesystem fsn = spy(setupFileSystem());
    final FSDirectory fsd = fsn.getFSDirectory();
    FSEditLog editlog = fsn.getEditLog();
    final boolean[] deleted = new boolean[]{false};

    doAnswer(new Answer<Void>() {

      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        if(!deleted[0]) {
          fsn.writeLock();
          try {
            INodesInPath iip = fsd.getINodesInPath(FILE_PATH, DirOp.READ);
            FSDirDeleteOp.delete(fsd, iip, new INode.BlocksMapUpdateInfo(),
                                 new ArrayList<INode>(), new ArrayList<Long>(),
                                 now());
          } finally {
            fsn.writeUnlock();
          }
          deleted[0] = true;
        }
        invocation.callRealMethod();
        return null;
      }
    }).when(fsn).checkOperation(OperationCategory.WRITE);
    fsn.getBlockLocations("dummy", RESERVED_PATH, 0, 1024);

    verify(editlog, never()).logTimes(anyString(), anyLong(), anyLong());
    fsn.close();
  }

  @Test(timeout = 30000)
  public void testGetBlockLocationsRacingWithRename() throws IOException {
    FSNamesystem fsn = spy(setupFileSystem());
    final FSDirectory fsd = fsn.getFSDirectory();
    FSEditLog editlog = fsn.getEditLog();
    final String DST_PATH = "/bar";
    final boolean[] renamed = new boolean[] {false};

    doAnswer(new Answer<Void>() {

      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        if (!renamed[0]) {
          fsn.writeLock();
          try {
            FSDirRenameOp.renameTo(fsd, fsd.getPermissionChecker(), FILE_PATH,
                                   DST_PATH, new INode.BlocksMapUpdateInfo(),
                                   false);
            renamed[0] = true;
          } finally {
            fsn.writeUnlock();
          }
        }
        invocation.callRealMethod();
        return null;
      }
    }).when(fsn).checkOperation(OperationCategory.WRITE);
    fsn.getBlockLocations("dummy", RESERVED_PATH, 0, 1024);

    verify(editlog).logTimes(eq(DST_PATH), anyLong(), anyLong());
    fsn.close();
  }

  private static FSNamesystem setupFileSystem() throws IOException {
    Configuration conf = new Configuration();
    conf.setLong(DFS_NAMENODE_ACCESSTIME_PRECISION_KEY, 1L);
    FSEditLog editlog = mock(FSEditLog.class);
    FSImage image = mock(FSImage.class);
    when(image.getEditLog()).thenReturn(editlog);
    final FSNamesystem fsn = new FSNamesystem(conf, image, true);

    PermissionStatus perm = new PermissionStatus(
        "hdfs", "supergroup",
        FsPermission.createImmutable((short) 0x1ff));
    final INodeFile file = new INodeFile(
        MOCK_INODE_ID, FILE_NAME.getBytes(StandardCharsets.UTF_8),
        perm, 1, 1, new BlockInfo[] {}, (short) 1,
        DFS_BLOCK_SIZE_DEFAULT);

    fsn.writeLock();
    try {
      final FSDirectory fsd = fsn.getFSDirectory();
      INodesInPath iip = fsd.getINodesInPath("/", DirOp.READ);
      fsd.addINode(iip, file, null);
    } finally {
      fsn.writeUnlock();
    }
    return fsn;
  }

  @Test
  public void testCallerContextAddBlocksForIDCGetBlockLocations() throws IOException {
    // Setup the cluster.
    final String[] hosts = {"host0", "host1", "host2"};
    final String[] racks = {"/dc0/rack0", "/dc0/rack1", "/dc0/rack2"};
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);
    conf.setBoolean(HADOOP_CALLER_CONTEXT_ENABLED_KEY, true);
    conf.setBoolean(DFS_NAMENODE_AUDIT_LOG_ADD_BLOCKS_ENABLED, true);
    conf.setClass(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY,
        BlockPlacementPolicyWithDataCenter.class,
        BlockPlacementPolicy.class);
    conf.setBoolean(DFSConfigKeys.DFS_USE_DFS_NETWORK_TOPOLOGY_KEY, true);
    conf.setClass(DFSConfigKeys.DFS_NET_TOPOLOGY_IMPL_KEY,
        DFSNetworkTopologyWithDataCenter.class,
        DFSNetworkTopology.class);

    try (MiniDFSCluster cluster = new MiniDFSCluster
        .Builder(conf).numDataNodes(hosts.length)
        .hosts(hosts).racks(racks).build()) {
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();
      org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path("/test.txt");
      DFSTestUtil.createFile(fs, path, 1024, 1024 * 3, 1024, (short)3,
          0L);

      // Create DatanodeManager instance.
      FSNamesystem fsn = cluster.getNamesystem();
      DatanodeManager dm = fsn.getBlockManager().getDatanodeManager();

      List<LocatedBlock> blocks = DFSTestUtil.getAllBlocks(fs, path);
      Assert.assertEquals(3, blocks.size());
      Assert.assertEquals(3, blocks.get(0).getLocations().length);
      String clientMachine = "host3";

      // Client in /dc0 and will not cross idc read.
      StaticMapping.addNodeToRack(clientMachine, "/dc0/rack1");
      LocatedBlocks locatedBlocks = fsn.getBlockLocations(clientMachine, path.toString(), 0,
          1024);
      Assert.assertEquals(1, locatedBlocks.getLocatedBlocks().size());
      Assert.assertNull(CallerContext.getCurrent());

      // Client in /dc1 and will cross idc read.
      StaticMapping.addNodeToRack(clientMachine, "/dc1/rack1");

      // Read data size is the size of one block,
      // the first and last blockId will be recorded in CallerContext.
      locatedBlocks = fsn.getBlockLocations(clientMachine, path.toString(), 0,
          1024);
      Assert.assertEquals(1, locatedBlocks.getLocatedBlocks().size());
      Assert.assertTrue(CallerContext.getCurrent().getContext().contains("blkIds:" +
          blocks.get(0).getBlock().getBlockId() + "$" +
          blocks.get(2).getBlock().getBlockId()));
      CallerContext.setCurrent(null);

      // Read data size is the size of two block,
      // the all blockId will be recorded in CallerContext.
      locatedBlocks = fsn.getBlockLocations(clientMachine, path.toString(), 0,
          1024 * 2);
      Assert.assertEquals(2, locatedBlocks.getLocatedBlocks().size());
      Assert.assertTrue(CallerContext.getCurrent().getContext().contains("blkIds:" +
          blocks.get(0).getBlock().getBlockId() + "$" +
          blocks.get(1).getBlock().getBlockId() + "$" +
          blocks.get(2).getBlock().getBlockId()));
      CallerContext.setCurrent(null);

      // Read data size is the size of three block,
      // the all blockId will be recorded in CallerContext.
      locatedBlocks = fsn.getBlockLocations(clientMachine, path.toString(), 0,
          1024 * 3);
      Assert.assertEquals(3, locatedBlocks.getLocatedBlocks().size());
      Assert.assertTrue(CallerContext.getCurrent().getContext().contains("blkIds:" +
          blocks.get(0).getBlock().getBlockId() + "$" +
          blocks.get(1).getBlock().getBlockId() + "$" +
          blocks.get(2).getBlock().getBlockId()));
      CallerContext.setCurrent(null);

      // Set enableAuditLogAddBlocks is false,
      // reading data, will not record blkIdList in CallerContext.
      dm.setEnableAuditLogAddBlocks(false);
      locatedBlocks = fsn.getBlockLocations(clientMachine, path.toString(), 0,
          1024 * 3);
      Assert.assertEquals(3, locatedBlocks.getLocatedBlocks().size());
      Assert.assertFalse(CallerContext.getCurrent().getContext().contains("blkIds"));
      CallerContext.setCurrent(null);
    }
  }
}
