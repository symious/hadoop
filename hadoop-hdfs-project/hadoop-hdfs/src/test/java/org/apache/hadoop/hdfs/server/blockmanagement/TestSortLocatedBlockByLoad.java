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

package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.net.DFSNetworkTopologyWithDataCenter;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.net.NetworkTopology;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertSame;


/**
 * This class tests the sorting of located blocks based on
 * multiple states.
 */
public class TestSortLocatedBlockByLoad {

  private static DatanodeManager mockDatanodeManager(
      boolean avoidHighLoadForRead, boolean multiDC)
      throws IOException {

    Configuration conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_AVOID_HIGH_LOAD_FOR_READ_KEY,
        avoidHighLoadForRead);
    if (!multiDC) {
      conf.setClass(DFSConfigKeys.DFS_NET_TOPOLOGY_IMPL_KEY,
          DFSConfigKeys.DFS_NET_TOPOLOGY_IMPL_DEFAULT, NetworkTopology.class);
    } else {
      conf.setClass(DFSConfigKeys.DFS_NET_TOPOLOGY_IMPL_KEY,
          DFSNetworkTopologyWithDataCenter.class, NetworkTopology.class);
    }
    FSNamesystem fsn = Mockito.mock(FSNamesystem.class);
    BlockManager bm = Mockito.mock(BlockManager.class);

    BlockReportLeaseManager blm = new BlockReportLeaseManager(conf);
    Mockito.when(bm.getBlockReportLeaseManager()).thenReturn(blm);

    return Mockito.spy(new DatanodeManager(bm, fsn, conf));
  }

  /**
   * mock nodes {
   * ipAddr      location  XceiverCount
   * ("1.1.1.1", "/d1/r1", 5000),
   * ("2.2.2.2", "/d1/r1", 3000),
   * ("3.3.3.3", "/d1/r1", 1000)
   * }
   * <p>
   * restuls {
   * ("3.3.3.3", "/d1/r1", 1000),
   * ("2.2.2.2", "/d1/r1", 3000),
   * ("1.1.1.1", "/d1/r1", 5000)
   * }
   */
  @Test
  public void testOneDataCentreNoMultiDC() throws IOException {
    int[] XceiverCounts = {
        5000,
        3000,
        1000
    };

    DatanodeDescriptor[] datanodeDescriptors = {
        DFSTestUtil.getDatanodeDescriptor("1.1.1.1", "/d1/r1"),
        DFSTestUtil.getDatanodeDescriptor("2.2.2.2", "/d1/r1"),
        DFSTestUtil.getDatanodeDescriptor("3.3.3.3", "/d1/r1"),
    };

    DatanodeManager dm = mockDatanodeManager(true, false);
    DatanodeInfo[] locs = new DatanodeInfo[datanodeDescriptors.length];

    for (int i = 0; i < datanodeDescriptors.length; i++) {
      dm.addDatanode(datanodeDescriptors[i]);
      locs[i] = dm.getDatanode(datanodeDescriptors[i]);
      locs[i].setXceiverCount(XceiverCounts[i]);
    }

    LocatedBlock block = new LocatedBlock(
        new ExtendedBlock("pool", Long.MIN_VALUE, 1024L, new Date().getTime()),
        locs);

    List<LocatedBlock> locatedBlocks = Collections.singletonList(block);

    dm.sortLocatedBlocks("1.1.1.1", locatedBlocks);

    DatanodeInfo[] sortedLocations = block.getLocations();

    assertSame(locs[2].getIpAddr(), sortedLocations[0].getIpAddr());
    assertSame(locs[1].getIpAddr(), sortedLocations[1].getIpAddr());
    assertSame(locs[0].getIpAddr(), sortedLocations[2].getIpAddr());
  }

  /**
   * mock nodes {
   * ipAddr      location  XceiverCount
   * ("1.1.1.1", "/d1/r1", 5000),
   * ("2.2.2.2", "/d1/r1", 3000),
   * ("3.3.3.3", "/d1/r1", 1000),
   * ("4.4.4.4", "/d2/r2", 100)
   * }
   * <p>
   * restuls {
   * ("4.4.4.4", "/d2/r2", 100),
   * ("3.3.3.3", "/d1/r1", 1000),
   * ("2.2.2.2", "/d1/r1", 3000),
   * ("1.1.1.1", "/d1/r1", 5000)
   * }
   */
  @Test
  public void testTwoDataCentresNoMultiDC() throws IOException {
    int[] XceiverCounts = {
        5000,
        3000,
        1000,
        100
    };

    DatanodeDescriptor[] datanodeDescriptors = {
        DFSTestUtil.getDatanodeDescriptor("1.1.1.1", "/d1/r1"),
        DFSTestUtil.getDatanodeDescriptor("2.2.2.2", "/d1/r1"),
        DFSTestUtil.getDatanodeDescriptor("3.3.3.3", "/d1/r1"),
        DFSTestUtil.getDatanodeDescriptor("4.4.4.4", "/d2/r2"),
    };

    DatanodeManager dm = mockDatanodeManager(true, false);
    DatanodeInfo[] locs = new DatanodeInfo[datanodeDescriptors.length];

    for (int i = 0; i < datanodeDescriptors.length; i++) {
      dm.addDatanode(datanodeDescriptors[i]);
      locs[i] = dm.getDatanode(datanodeDescriptors[i]);
      locs[i].setXceiverCount(XceiverCounts[i]);
    }

    LocatedBlock block = new LocatedBlock(
        new ExtendedBlock("pool", Long.MIN_VALUE, 1024L, new Date().getTime()),
        locs);

    List<LocatedBlock> locatedBlocks = Collections.singletonList(block);

    dm.sortLocatedBlocks("1.1.1.1", locatedBlocks);

    DatanodeInfo[] sortedLocations = block.getLocations();

    assertSame(locs[3].getIpAddr(), sortedLocations[0].getIpAddr());
    assertSame(locs[2].getIpAddr(), sortedLocations[1].getIpAddr());
    assertSame(locs[1].getIpAddr(), sortedLocations[2].getIpAddr());
    assertSame(locs[0].getIpAddr(), sortedLocations[3].getIpAddr());
  }

  /**
   * mock nodes {
   * ipAddr      location  XceiverCount
   * ("1.1.1.1", "/d1/r1", 5000),
   * ("2.2.2.2", "/d1/r1", 3000),
   * ("3.3.3.3", "/d1/r1", 1000),
   * ("4.4.4.4", "/d2/r2", 100)
   * }
   * <p>
   * restuls {
   * ("3.3.3.3", "/d1/r1", 1000),
   * ("2.2.2.2", "/d1/r1", 3000),
   * ("1.1.1.1", "/d1/r1", 5000),
   * ("4.4.4.4", "/d2/r2", 100),
   * }
   */
  @Test
  public void testTwoDataCentresMultiDC() throws IOException {
    int[] XceiverCounts = {
        5000,
        3000,
        1000,
        100
    };

    DatanodeDescriptor[] datanodeDescriptors = {
        DFSTestUtil.getDatanodeDescriptor("1.1.1.1", "/d1/r1"),
        DFSTestUtil.getDatanodeDescriptor("2.2.2.2", "/d1/r1"),
        DFSTestUtil.getDatanodeDescriptor("3.3.3.3", "/d1/r1"),
        DFSTestUtil.getDatanodeDescriptor("4.4.4.4", "/d2/r2"),
    };

    DatanodeManager dm = mockDatanodeManager(true, true);
    DatanodeInfo[] locs = new DatanodeInfo[datanodeDescriptors.length];

    for (int i = 0; i < datanodeDescriptors.length; i++) {
      dm.addDatanode(datanodeDescriptors[i]);
      locs[i] = dm.getDatanode(datanodeDescriptors[i]);
      locs[i].setXceiverCount(XceiverCounts[i]);
    }

    LocatedBlock block = new LocatedBlock(
        new ExtendedBlock("pool", Long.MIN_VALUE, 1024L, new Date().getTime()),
        locs);

    List<LocatedBlock> locatedBlocks = Collections.singletonList(block);

    dm.sortLocatedBlocks("1.1.1.1", locatedBlocks);

    DatanodeInfo[] sortedLocations = block.getLocations();

    assertSame(locs[2].getIpAddr(), sortedLocations[0].getIpAddr());
    assertSame(locs[1].getIpAddr(), sortedLocations[1].getIpAddr());
    assertSame(locs[0].getIpAddr(), sortedLocations[2].getIpAddr());
    assertSame(locs[3].getIpAddr(), sortedLocations[3].getIpAddr());
  }

  /**
   * mock nodes {
   * ipAddr      location  XceiverCount
   * ("1.1.1.1", "/d1/r1", 5000),
   * ("2.2.2.2", "/d1/r1", 3000),
   * ("3.3.3.3", "/d1/r1", 1000),
   * ("4.4.4.4", "/d2/r2", 100)
   * }
   * <p>
   * results dmNoMultiDC {
   * ("4.4.4.4", "/d2/r2", 100),
   * ("3.3.3.3", "/d1/r1", 1000),
   * ("2.2.2.2", "/d1/r1", 3000),
   * ("1.1.1.1", "/d1/r1", 5000)
   * }
   * <p>
   * results dmMultiDC {
   * ("3.3.3.3", "/d1/r1", 1000),
   * ("2.2.2.2", "/d1/r1", 3000),
   * ("1.1.1.1", "/d1/r1", 5000),
   * ("4.4.4.4", "/d2/r2", 100),
   * }
   */
  @Test
  public void testTwoDataCentresNoMultiDCAndMultiDC() throws IOException {
    int[] XceiverCounts = {
        5000,
        3000,
        1000,
        100
    };

    DatanodeDescriptor[] datanodeDescriptors = {
        DFSTestUtil.getDatanodeDescriptor("1.1.1.1", "/d1/r1"),
        DFSTestUtil.getDatanodeDescriptor("2.2.2.2", "/d1/r1"),
        DFSTestUtil.getDatanodeDescriptor("3.3.3.3", "/d1/r1"),
        DFSTestUtil.getDatanodeDescriptor("4.4.4.4", "/d2/r2"),
    };

    DatanodeManager dmNoMultiDC = mockDatanodeManager(true, false);
    DatanodeManager dmMultiDC = mockDatanodeManager(true, true);
    DatanodeInfo[] locsNoMultiDC = new DatanodeInfo[datanodeDescriptors.length];
    DatanodeInfo[] locsMultiDC = new DatanodeInfo[datanodeDescriptors.length];

    for (int i = 0; i < datanodeDescriptors.length; i++) {
      dmNoMultiDC.addDatanode(datanodeDescriptors[i]);
      locsNoMultiDC[i] = dmNoMultiDC.getDatanode(datanodeDescriptors[i]);
      locsNoMultiDC[i].setXceiverCount(XceiverCounts[i]);

      dmMultiDC.addDatanode(datanodeDescriptors[i]);
      locsMultiDC[i] = dmMultiDC.getDatanode(datanodeDescriptors[i]);
      locsMultiDC[i].setXceiverCount(XceiverCounts[i]);
    }

    LocatedBlock block1 = new LocatedBlock(
        new ExtendedBlock("pool", Long.MIN_VALUE, 1024L, new Date().getTime()),
        locsNoMultiDC);
    LocatedBlock block2 = new LocatedBlock(
        new ExtendedBlock("pool", Long.MIN_VALUE, 1024L, new Date().getTime()),
        locsMultiDC);

    List<LocatedBlock> locatedBlocksNoMultiDC =
        Collections.singletonList(block1);
    List<LocatedBlock> locatedBlocksMultiDC =
        Collections.singletonList(block2);

    dmNoMultiDC.sortLocatedBlocks("1.1.1.1", locatedBlocksNoMultiDC);
    dmMultiDC.sortLocatedBlocks("1.1.1.1", locatedBlocksMultiDC);

    DatanodeInfo[] sortedLocationsNoMultiDC = block1.getLocations();
    DatanodeInfo[] sortedLocationsMultiDC = block2.getLocations();

    assertSame(locsNoMultiDC[3].getIpAddr(), sortedLocationsNoMultiDC[0].getIpAddr());
    assertSame(locsNoMultiDC[2].getIpAddr(), sortedLocationsNoMultiDC[1].getIpAddr());
    assertSame(locsNoMultiDC[1].getIpAddr(), sortedLocationsNoMultiDC[2].getIpAddr());
    assertSame(locsNoMultiDC[0].getIpAddr(), sortedLocationsNoMultiDC[3].getIpAddr());

    assertSame(locsMultiDC[2].getIpAddr(), sortedLocationsMultiDC[0].getIpAddr());
    assertSame(locsMultiDC[1].getIpAddr(), sortedLocationsMultiDC[1].getIpAddr());
    assertSame(locsMultiDC[0].getIpAddr(), sortedLocationsMultiDC[2].getIpAddr());
    assertSame(locsMultiDC[3].getIpAddr(), sortedLocationsMultiDC[3].getIpAddr());
  }
}