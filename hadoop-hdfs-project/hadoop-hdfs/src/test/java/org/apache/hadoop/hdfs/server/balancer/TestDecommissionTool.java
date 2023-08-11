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
package org.apache.hadoop.hdfs.server.balancer;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.StripedFileTestUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

public class TestDecommissionTool {

  private DatanodeInfo source;
  private DatanodeInfo sameRack;
  private DatanodeInfo otherRack1;
  private DatanodeInfo otherRack2;
  private DatanodeInfo otherRack3;
  private DatanodeInfo otherDC1;
  private DatanodeInfo otherDC2;
  BlocksWithLocations.BlockWithLocations blockWithLocations;
  private final String blockPool = "mockBlockPool";

  @Before
  public void setup() {
    Block block = Mockito.mock(Block.class);
    Mockito.doReturn("blk_1234_567890").when(block).toString();
    blockWithLocations =
        new BlocksWithLocations.BlockWithLocations(block, null, null, null);

    source = Mockito.mock(DatanodeInfo.class);
    Mockito.doReturn("/DC1/Rack1").when(source).getNetworkLocation();
    Mockito.doReturn("source").when(source).getDatanodeUuid();
    Mockito.doReturn("/DC1/Rack1/source").when(source).toString();

    sameRack = Mockito.mock(DatanodeInfo.class);
    Mockito.doReturn("/DC1/Rack1").when(sameRack).getNetworkLocation();
    Mockito.doReturn("sameRack").when(sameRack).getDatanodeUuid();
    Mockito.doReturn("/DC1/Rack1/sameRack").when(sameRack).toString();

    otherRack1 = Mockito.mock(DatanodeInfo.class);
    Mockito.doReturn("/DC1/Rack2").when(otherRack1).getNetworkLocation();
    Mockito.doReturn("otherRack1").when(otherRack1).getDatanodeUuid();
    Mockito.doReturn("/DC1/Rack2/otherRack1").when(otherRack1).toString();

    otherRack2 = Mockito.mock(DatanodeInfo.class);
    Mockito.doReturn("/DC1/Rack2").when(otherRack2).getNetworkLocation();
    Mockito.doReturn("otherRack2").when(otherRack2).getDatanodeUuid();
    Mockito.doReturn("/DC1/Rack2/otherRack2").when(otherRack2).toString();

    otherRack3 = Mockito.mock(DatanodeInfo.class);
    Mockito.doReturn("/DC1/Rack3").when(otherRack3).getNetworkLocation();
    Mockito.doReturn("otherRack3").when(otherRack3).getDatanodeUuid();
    Mockito.doReturn("/DC1/Rack3/otherRack3").when(otherRack3).toString();

    otherDC1 = Mockito.mock(DatanodeInfo.class);
    Mockito.doReturn("/DC2/Rack1").when(otherDC1).getNetworkLocation();
    Mockito.doReturn("otherDC1").when(otherDC1).getDatanodeUuid();
    Mockito.doReturn("/DC2/Rack2/otherDC1").when(otherDC1).toString();

    otherDC2 = Mockito.mock(DatanodeInfo.class);
    Mockito.doReturn("/DC2/Rack1").when(otherDC2).getNetworkLocation();
    Mockito.doReturn("otherDC2").when(otherDC2).getDatanodeUuid();
    Mockito.doReturn("/DC2/Rack1/otherDC2").when(otherDC2).toString();
  }

  @Test
  public void testTaskOtherRack1() {
    DatanodeInfo target = Mockito.mock(DatanodeInfo.class);
    Mockito.doReturn(otherRack1.getNetworkLocation()).when(target).getNetworkLocation();
    Mockito.doReturn("target").when(target).getDatanodeUuid();
    Mockito.doReturn(otherRack1.getNetworkLocation() + "/target").when(target).toString();

    List<DatanodeInfo> locations = new ArrayList<>();
    locations.add(source);
    locations.add(sameRack);
    locations.add(otherRack1);

    int count = 50;
    while (count-- > 0) {
      DatanodeInfo proxy = DecommissionTool.chooseProxy(blockWithLocations, source, target, null,
          locations, true, blockPool);
      Assert.assertEquals(otherRack1, proxy);
    }
  }

  @Test
  public void testTaskOtherRack2() {
    DatanodeInfo target = Mockito.mock(DatanodeInfo.class);
    Mockito.doReturn(otherRack3.getNetworkLocation()).when(target).getNetworkLocation();
    Mockito.doReturn("target").when(target).getDatanodeUuid();
    Mockito.doReturn(otherRack3.getNetworkLocation() + "/target").when(target).toString();

    List<DatanodeInfo> locations = new ArrayList<>();
    locations.add(source);
    locations.add(sameRack);
    locations.add(otherRack1);

    int count = 50;
    while (count-- > 0) {
      DatanodeInfo proxy = DecommissionTool.chooseProxy(blockWithLocations, source, target, null,
          locations, true, blockPool);
      Assert.assertTrue(DecommissionTool.isSameRack(source, proxy) ||
          DecommissionTool.isSameRack(otherRack1, proxy));
    }
  }

  @Test
  public void testTaskSameRack() {
    DatanodeInfo target = Mockito.mock(DatanodeInfo.class);
    Mockito.doReturn(source.getNetworkLocation()).when(target).getNetworkLocation();
    Mockito.doReturn("target").when(target).getDatanodeUuid();
    Mockito.doReturn(source.getNetworkLocation() + "/target").when(target).toString();

    List<DatanodeInfo> locations = new ArrayList<>();
    locations.add(source);
    locations.add(otherRack1);
    locations.add(otherRack2);

    int count = 50;
    while (count-- > 0) {
      DatanodeInfo proxy = DecommissionTool.chooseProxy(blockWithLocations, source, target, null,
          locations, true, blockPool);
      Assert.assertTrue(DecommissionTool.isSameRack(source, proxy) ||
          DecommissionTool.isSameRack(otherRack1, proxy));
    }
  }

  @Test
  public void testTaskSameRack2() {
    DatanodeInfo target = Mockito.mock(DatanodeInfo.class);
    Mockito.doReturn(source.getNetworkLocation()).when(target).getNetworkLocation();
    Mockito.doReturn("target").when(target).getDatanodeUuid();
    Mockito.doReturn(source.getNetworkLocation() + "/target").when(target).toString();

    List<DatanodeInfo> locations = new ArrayList<>();
    locations.add(source);
    locations.add(otherRack1);
    locations.add(otherDC1);

    int count = 50;
    while (count-- > 0) {
      DatanodeInfo proxy = DecommissionTool.chooseProxy(blockWithLocations, source, target, null,
          locations, true, blockPool);
      Assert.assertTrue(DecommissionTool.isSameRack(source, proxy) ||
          DecommissionTool.isSameRack(otherRack1, proxy));
    }
  }

  @Test
  public void testTaskSameDC() {
    DatanodeInfo target = Mockito.mock(DatanodeInfo.class);
    Mockito.doReturn(source.getNetworkLocation()).when(target).getNetworkLocation();
    Mockito.doReturn("target").when(target).getDatanodeUuid();
    Mockito.doReturn(source.getNetworkLocation() + "/target").when(target).toString();

    List<DatanodeInfo> locations = new ArrayList<>();
    locations.add(source);
    locations.add(otherDC1);
    locations.add(otherDC2);

    int count = 50;
    while (count-- > 0) {
      DatanodeInfo proxy = DecommissionTool.chooseProxy(blockWithLocations, source, target, null,
          locations, false, blockPool);
      Assert.assertEquals(source, proxy);
    }
  }

  @Test
  public void testTaskSameDC2() {
    DatanodeInfo target = Mockito.mock(DatanodeInfo.class);
    Mockito.doReturn(source.getNetworkLocation()).when(target).getNetworkLocation();
    Mockito.doReturn("target").when(target).getDatanodeUuid();
    Mockito.doReturn(source.getNetworkLocation() + "/target").when(target).toString();

    List<DatanodeInfo> locations = new ArrayList<>();
    locations.add(source);
    locations.add(otherDC1);
    locations.add(otherDC2);

    int count = 50;
    while (count-- > 0) {
      DatanodeInfo proxy = DecommissionTool.chooseProxy(blockWithLocations, source, target, null,
          locations, true, blockPool);
      Assert.assertTrue(DecommissionTool.isSameRack(source, proxy) ||
          DecommissionTool.isSameRack(otherDC1, proxy));
    }
  }

  @Test
  public void testSameRack3() {
    final String[] datanodeUuids = {"dn1", "dn2", "dn3"};
    final String[] storageIDs = {"s1", "s2", "s3"};
    final StorageType[] storageTypes = {
        StorageType.DISK, StorageType.DISK, StorageType.DISK};
    final byte[] indices = {0, 1, 2};
    final short dataBlkNum = 6;
    BlocksWithLocations.BlockWithLocations
        blkLocs = new BlocksWithLocations.BlockWithLocations(new Block(-1, 0, 1),
        datanodeUuids, storageIDs, storageTypes);
    BlocksWithLocations.BlockWithLocations blockWithLocations = new
        BlocksWithLocations.StripedBlockWithLocations(blkLocs, indices, dataBlkNum,
        StripedFileTestUtil.getDefaultECPolicy().getCellSize());

    DatanodeInfo target = Mockito.mock(DatanodeInfo.class);
    Mockito.doReturn(otherRack3.getNetworkLocation()).when(target).getNetworkLocation();
    Mockito.doReturn("target").when(target).getDatanodeUuid();
    Mockito.doReturn(otherRack3.getNetworkLocation() + "/target").when(target).toString();

    List<DatanodeInfo> locations = new ArrayList<>();
    locations.add(source);
    locations.add(otherDC1);
    locations.add(otherDC2);

    int count = 50;
    while (count-- > 0) {
      DatanodeInfo proxy = DecommissionTool.chooseProxy(blockWithLocations, source, target, null,
          locations, true, blockPool);
      Assert.assertEquals(source, proxy);
    }
  }
}
