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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.net.DFSNetworkTopology;
import org.apache.hadoop.hdfs.net.DFSNetworkTopologyWithDataCenter;
import org.apache.hadoop.hdfs.util.RwLockMode;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class TestReplicationPolicyConsiderLoadWithDataCenter
    extends BaseReplicationPolicyTest {

  public TestReplicationPolicyConsiderLoadWithDataCenter(String blockPlacementPolicy) {
    this.blockPlacementPolicy = blockPlacementPolicy;
  }

  @Parameterized.Parameters
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[][] {
        { BlockPlacementPolicyWithDataCenter.class.getName() } });
  }

  @Override
  DatanodeDescriptor[] getDatanodeDescriptors(Configuration conf) {
    conf.setClass(DFSConfigKeys.DFS_NET_TOPOLOGY_IMPL_KEY,
        DFSNetworkTopologyWithDataCenter.class,
        DFSNetworkTopology.class);
    conf.setDouble(
        DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_FACTOR, 1.2);
    final String[] racks = {
        "/dc1/rack1",
        "/dc1/rack1",
        "/dc1/rack2",
        "/dc1/rack2",
        "/dc2/rack1",
        "/dc2/rack1",
        "/dc2/rack2",
        "/dc2/rack2"};
    storages = DFSTestUtil.createDatanodeStorageInfos(racks);
    return DFSTestUtil.toDatanodeDescriptor(storages);
  }

  @Test
  public void testConsiderLoadFactor() throws IOException {
    namenode.getNamesystem().writeLock(RwLockMode.BM, "testConsiderLoadFactor");
    try {
      dnManager.getHeartbeatManager().updateHeartbeat(dataNodes[0],
          BlockManagerTestUtil.getStorageReportsForDatanode(dataNodes[0]),
          dataNodes[0].getCacheCapacity(),
          dataNodes[0].getCacheUsed(),
          15, 0, null);
      dnManager.getHeartbeatManager().updateHeartbeat(dataNodes[1],
          BlockManagerTestUtil.getStorageReportsForDatanode(dataNodes[1]),
          dataNodes[1].getCacheCapacity(),
          dataNodes[1].getCacheUsed(),
          15, 0, null);
      dnManager.getHeartbeatManager().updateHeartbeat(dataNodes[2],
          BlockManagerTestUtil.getStorageReportsForDatanode(dataNodes[2]),
          dataNodes[2].getCacheCapacity(),
          dataNodes[2].getCacheUsed(),
          15, 0, null);
      dnManager.getHeartbeatManager().updateHeartbeat(dataNodes[3],
          BlockManagerTestUtil.getStorageReportsForDatanode(dataNodes[3]),
          dataNodes[3].getCacheCapacity(),
          dataNodes[3].getCacheUsed(),
          15, 0, null);

      dnManager.getHeartbeatManager().updateHeartbeat(dataNodes[4],
          BlockManagerTestUtil.getStorageReportsForDatanode(dataNodes[4]),
          dataNodes[4].getCacheCapacity(),
          dataNodes[4].getCacheUsed(),
          5, 0, null);
      dnManager.getHeartbeatManager().updateHeartbeat(dataNodes[5],
          BlockManagerTestUtil.getStorageReportsForDatanode(dataNodes[5]),
          dataNodes[5].getCacheCapacity(),
          dataNodes[5].getCacheUsed(),
          5, 0, null);
      dnManager.getHeartbeatManager().updateHeartbeat(dataNodes[6],
          BlockManagerTestUtil.getStorageReportsForDatanode(dataNodes[6]),
          dataNodes[6].getCacheCapacity(),
          dataNodes[6].getCacheUsed(),
          5, 0, null);
      dnManager.getHeartbeatManager().updateHeartbeat(dataNodes[7],
          BlockManagerTestUtil.getStorageReportsForDatanode(dataNodes[7]),
          dataNodes[7].getCacheCapacity(),
          dataNodes[7].getCacheUsed(),
          5, 0, null);
      //Add values in above heartbeats
      double dc1Load = 15;
      double dc2Load = 5;

      double EPSILON = 0.0001;
      assertEquals(dc1Load, dnManager.getFSClusterStats()
          .getDataCenterInServiceXceiverAverage("/dc1"), EPSILON);
      assertEquals(dc2Load, dnManager.getFSClusterStats()
          .getDataCenterInServiceXceiverAverage("/dc2"), EPSILON);
    } finally {
      namenode.getNamesystem().writeUnlock(RwLockMode.BM, "testConsiderLoadFactor");
    }
  }
}