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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerFaultInjector;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestHAFailoverRace {
  private MiniQJMHACluster cluster;

  @Before
  public void setUpCluster() throws IOException {
    Configuration conf = new HdfsConfiguration();
    // Try to remove invalided corrupted blocks during starting active services.
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_REMOVE_CORRUPTED_BLOCKS_KEY, true);

    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 10 * 1024 * 1024);
    MiniQJMHACluster.Builder builder = new MiniQJMHACluster.Builder(conf).setNumNameNodes(2);
    builder.getDfsBuilder().numDataNodes(3);
    cluster = builder.build();
    cluster.getDfsCluster().waitActive();
    cluster.getDfsCluster().transitionToActive(0);
  }

  @After
  public void tearDownCluster() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testCorruptedBlockAfterHAFailover() throws Exception {
    AtomicBoolean enableBlockReceiveAndDeleted = new AtomicBoolean(false);
    BlockManagerFaultInjector.instance = new BlockManagerFaultInjector() {
      @Override
      public void mockDelayBlockReceiveAndDelete(String state) {
        if ("standby".equals(state)) {
          try {
            while (!enableBlockReceiveAndDeleted.get()) {
              System.out.println("I'm waiting.....");
              Thread.sleep(100);
            }
          } catch (Throwable e) {
            Assert.fail();
          }
        }
      }
    };

    DistributedFileSystem fs0 = cluster.getDfsCluster().getFileSystem(0);
    DistributedFileSystem fs1 = cluster.getDfsCluster().getFileSystem(1);
    FSNamesystem ns1 = cluster.getDfsCluster().getNamesystem(1);
    // Step1: Create one file
    Path testPath = new Path("/testCorruptedBlockAfterHAFailover");
    DFSTestUtil.createFile(fs0, testPath, 1024 * 1024, (short) 3,
        Time.monotonicNow());
    LocatedBlocks locatedBlocks = fs0.getClient().getLocatedBlocks(
        testPath.toUri().getPath(), 0, Long.MAX_VALUE);

    Assert.assertEquals(1, locatedBlocks.getLocatedBlocks().size());
    Assert.assertEquals(3, locatedBlocks.get(0).getLocations().length);
    ExtendedBlock lastBlock = locatedBlocks.get(0).getBlock();

    DFSTestUtil.appendFile(fs0, testPath, 1024 * 1024);
    locatedBlocks = fs0.getClient().getLocatedBlocks(
        testPath.toUri().getPath(), 0, Long.MAX_VALUE);
    Assert.assertEquals(1, locatedBlocks.getLocatedBlocks().size());
    Assert.assertEquals(3, locatedBlocks.get(0).getLocations().length);
    ExtendedBlock lastBlock1 = locatedBlocks.get(0).getBlock();
    Assert.assertTrue(lastBlock1.getGenerationStamp() > lastBlock.getGenerationStamp());

    DFSTestUtil.appendFile(fs0, testPath, 1024 * 1024);
    locatedBlocks = fs0.getClient().getLocatedBlocks(
        testPath.toUri().getPath(), 0, Long.MAX_VALUE);
    Assert.assertEquals(1, locatedBlocks.getLocatedBlocks().size());
    Assert.assertEquals(3, locatedBlocks.get(0).getLocations().length);
    ExtendedBlock lastBlock2 = locatedBlocks.get(0).getBlock();
    Assert.assertTrue(lastBlock2.getGenerationStamp() > lastBlock1.getGenerationStamp());
    Assert.assertEquals(3 * 1024 * 1024, lastBlock2.getNumBytes());

    cluster.getDfsCluster().transitionToStandby(0);
    ns1.getEditLogTailer().doTailEdits();

    enableBlockReceiveAndDeleted.set(true);

    LambdaTestUtils.await(300000, 100,
        () -> ns1.getPendingDataNodeMessageCount() == 3);

    cluster.getDfsCluster().transitionToActive(1);
    Assert.assertEquals(0, ns1.getBlockManager().getCorruptBlocks());
    DFSTestUtil.appendFile(fs1, testPath, 1024 * 1024);
  }
}
