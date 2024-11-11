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
package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

import static org.apache.hadoop.hdfs.server.blockmanagement.DataNodeBlockRecoveryTriggerHandler.DATANODE_BLOCK_RECOVERY_TRIGGER;

public class TestBlockRecoveryTrigger {

  MiniDFSCluster cluster;
  private DFSAdmin admin;

  @Before
  public void setup() throws IOException {
    HdfsConfiguration conf = new HdfsConfiguration();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();

    admin = new DFSAdmin(conf);
  }

  @After
  public void shutDownCluster() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testBlockRecoveryTrigger() throws IOException {
    DistributedFileSystem dfs = cluster.getFileSystem();
    Path testPath = new Path("/testBlockRecoveryTrigger.txt");
    DFSTestUtil.createFile(dfs, testPath, 1024, (short)3, 0);

    BlockLocation[] blockLocations = dfs.getFileBlockLocations(testPath, 0, Long.MAX_VALUE);
    Assert.assertEquals(1, blockLocations.length);
    Assert.assertEquals(3, blockLocations[0].getHosts().length);

    ArrayList<DataNode> dataNodes = cluster.getDataNodes();
    Assert.assertEquals(3, dataNodes.size());
    DataNode shutdownDN = dataNodes.get(0);
    String dnUUID = shutdownDN.getDatanodeUuid();
    shutdownDN.shutdown();

    FSNamesystem fsNamesystem = cluster.getNamesystem(0);
    DatanodeManager dnManager = fsNamesystem.getBlockManager().getDatanodeManager();

    // This DN is not yet marked as dead by NameNode.
    Assert.assertTrue(dnManager.getDatanode(dnUUID).isAlive());

    String [] args = new String[]{"-refresh", "localhost:" +
        cluster.getNameNodePort(), DATANODE_BLOCK_RECOVERY_TRIGGER,
        shutdownDN.getDisplayName()};
    int exitCode = admin.run(args);
    Assert.assertEquals("DFSAdmin should succeed", 0, exitCode);

    Assert.assertFalse(dnManager.getDatanode(dnUUID).isAlive());
    Assert.assertFalse(dnManager.isDatanodeDead(dnManager.getDatanode(dnUUID)));
  }
}
