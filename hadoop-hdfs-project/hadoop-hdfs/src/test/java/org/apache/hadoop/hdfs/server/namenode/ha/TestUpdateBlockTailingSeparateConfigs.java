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
package org.apache.hadoop.hdfs.server.namenode.ha;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_OBSERVER_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_STANDBY_KEY;
import static org.junit.Assert.assertEquals;

/**
 * Tests the race condition that IBR and update block may result
 * in inconsistent block genstamp.
 */
public class TestUpdateBlockTailingSeparateConfigs {
  private static final String TEST_DIR = "/TestUpdateBlockTailing";

  private static MiniQJMHACluster qjmhaCluster;
  private static MiniDFSCluster dfsCluster;
  private static DistributedFileSystem dfs;
  private static FSNamesystem[] fsns;

  @BeforeClass
  public static void startUpCluster() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(DFS_HA_TAILEDITS_INPROGRESS_KEY, true);
    conf.setBoolean(DFS_HA_TAILEDITS_INPROGRESS_STANDBY_KEY, false);
    conf.setBoolean(DFS_HA_TAILEDITS_INPROGRESS_OBSERVER_KEY, true);
    MiniQJMHACluster.Builder qjmBuilder =
        new MiniQJMHACluster.Builder(conf).setNumNameNodes(3);
    qjmBuilder.getDfsBuilder().numDataNodes(1);
    qjmhaCluster = qjmBuilder.build();
    dfsCluster = qjmhaCluster.getDfsCluster();
    dfsCluster.waitActive();
    dfsCluster.transitionToActive(0);
    dfs = dfsCluster.getFileSystem(0);
    fsns = new FSNamesystem[dfsCluster.getNumNameNodes()];
    for (int i = 0; i < dfsCluster.getNumNameNodes(); i++) {
      fsns[i] = dfsCluster.getNameNode(i).getNamesystem();
    }
    dfs.mkdirs(new Path(TEST_DIR), new FsPermission("755"));
  }

  @Before
  public void resetAllToStandby() throws IOException {
    dfsCluster.transitionToStandby(0);
    dfsCluster.transitionToStandby(1);
    dfsCluster.transitionToStandby(2);
  }

  @AfterClass
  public static void shutDownCluster() throws IOException {
    if (qjmhaCluster != null) {
      qjmhaCluster.shutdown();
    }
  }

  @Test
  public void testStandbyAppendBlockObserverNoInMemory() throws Exception {
    dfsCluster.transitionToActive(0);
    dfsCluster.transitionToObserver(1);
    dfsCluster.transitionToStandby(2);

    final String testFile =
        TEST_DIR + "/testStandbyAppendBlockObserverNoInMemory";
    final long fileLen = 1 << 16;
    long startingStamp = NameNodeAdapter.getGenerationStamp(fsns[0]);

    DFSTestUtil.createFile(dfs, new Path(testFile), fileLen, (short) 1, 0);
    verifyTailEdits(0, startingStamp, new int[] { 1, 1, 0 });

    appendBlock(dfs, testFile);
    verifyTailEdits(0, startingStamp, new int[] { 2, 2, 0 });

    fsns[2].getEditLogTailer().triggerActiveLogRoll();
    verifyTailEdits(0, startingStamp, new int[] { 2, 2, 2 });

    final ClientProtocol rpc0 = dfsCluster.getNameNode(0).getRpcServer();
    rpc0.delete(testFile, false);
  }

  @Test
  public void testStandbyAppendBlockObserverNoInMemoryWithFailOvers()
      throws Exception {
    dfsCluster.transitionToActive(0);
    dfsCluster.transitionToStandby(1);
    dfsCluster.transitionToObserver(2);

    final String testFile =
        TEST_DIR + "/testStandbyAppendBlockObserverNoInMemory";
    final long fileLen = 1 << 16;

    long startingStamp = NameNodeAdapter.getGenerationStamp(fsns[0]);

    // Start with NN0 active, NN1 standby, NN2 obs
    DFSTestUtil.createFile(dfs, new Path(testFile), fileLen, (short) 1, 0);
    verifyTailEdits(0, startingStamp, new int[] { 1, 0, 1 });

    // Standby falling behind
    appendBlock(dfs, testFile);
    verifyTailEdits(0, startingStamp, new int[] { 2, 0, 2 });

    // Transition to NN0 standby, NN1 active
    dfsCluster.transitionToStandby(0);
    dfsCluster.transitionToActive(1);
    verifyTailEdits(1, startingStamp, new int[] { 2, 2, 2 });

    // Standby (now is NN0) falling behind again
    appendBlock(dfsCluster.getFileSystem(1), testFile);
    verifyTailEdits(1, startingStamp, new int[] { 2, 3, 3 });

    dfsCluster.transitionToStandby(1);
    dfsCluster.transitionToActive(0);
    verifyTailEdits(0, startingStamp, new int[] { 3, 3, 3 });

    // Standby falling behind
    appendBlock(dfs, testFile);
    verifyTailEdits(0, startingStamp, new int[] { 4, 3, 4 });

    // Transition to NN1 obs, NN2 standby
    dfsCluster.transitionToStandby(2);
    dfsCluster.transitionToObserver(1);
    appendBlock(dfs, testFile);
    verifyTailEdits(0, startingStamp, new int[] { 5, 5, 4 });

    dfsCluster.transitionToStandby(0);
    dfsCluster.transitionToObserver(0);
    dfsCluster.transitionToStandby(1);
    dfsCluster.transitionToActive(2);
    verifyTailEdits(2, startingStamp, new int[] { 5, 5, 5 });

    appendBlock(dfsCluster.getFileSystem(2), testFile);
    verifyTailEdits(2, startingStamp, new int[] { 6, 5, 6 });

    final ClientProtocol rpc = dfsCluster.getNameNode(2).getRpcServer();
    rpc.delete(testFile, false);
  }

  private void appendBlock(DistributedFileSystem dfs, String testFile)
      throws IOException {
    // Append block without newBlock flag
    try (FSDataOutputStream out = dfs.append(new Path(testFile))) {
      final byte[] data = new byte[1 << 16];
      ThreadLocalRandom.current().nextBytes(data);
      out.write(data);
    }
  }

  private void verifyTailEdits(int activeNNIdx, long startingStamp,
      int[] offsets) throws IOException, InterruptedException {
    assert dfsCluster.getNumNameNodes() == offsets.length;
    fsns[activeNNIdx].getEditLog().logSync();
    for (int i = 0; i < dfsCluster.getNumNameNodes(); i++) {
      if (i != activeNNIdx) {
        fsns[i].getEditLogTailer().doTailEdits();
      }
    }
    for (int i = 0; i < dfsCluster.getNumNameNodes(); i++) {
      assertEquals(
          String.format("Failed Generation Stamp check on namenode %d", i),
          startingStamp + offsets[i],
          NameNodeAdapter.getGenerationStamp(fsns[i]));
    }
  }
}
