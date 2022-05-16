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
package org.apache.hadoop.hdfs.net;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.net.Node;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestDFSNetworkTopologyWithDataCenter {
  private final static DFSNetworkTopologyWithDataCenter CLUSTER =
      DFSNetworkTopologyWithDataCenter.getInstance(new Configuration());

  @Rule
  public Timeout testTimeout = new Timeout(30000);

  @Before
  public void setupDatanodes() {
    final String[] racks = {
        "/d1/r1", "/d1/r1", "/d1/r2", "/d1/r2", "/d1/r2",

        "/d2/r3", "/d2/r3", "/d2/r3",

        "/d3/r1", "/d3/r2", "/d3/r3", "/d3/r4", "/d3/r4",

        "/d4/r1", "/d4/r1", "/d4/r1", "/d4/r1", "/d4/r1",
        "/d4/r1", "/d4/r1"};
    final String[] hosts = {
        "host1", "host2", "host3", "host4", "host5",
        "host6", "host7", "host8",
        "host9", "host10", "host11", "host12", "host13",
        "host14", "host15", "host16", "host17", "host18",
        "host19", "host20"};
    final StorageType[] types = {
        StorageType.ARCHIVE, StorageType.DISK, StorageType.ARCHIVE,
        StorageType.DISK, StorageType.DISK,

        StorageType.DISK, StorageType.RAM_DISK, StorageType.SSD,

        StorageType.DISK, StorageType.RAM_DISK, StorageType.DISK,
        StorageType.ARCHIVE, StorageType.ARCHIVE,

        StorageType.DISK, StorageType.DISK, StorageType.RAM_DISK,
        StorageType.RAM_DISK, StorageType.ARCHIVE, StorageType.ARCHIVE,
        StorageType.SSD};
    final DatanodeStorageInfo[] storages =
        DFSTestUtil.createDatanodeStorageInfos(20, racks, hosts, types);
    DatanodeDescriptor[] dataNodes = DFSTestUtil.toDatanodeDescriptor(storages);
    for (DatanodeDescriptor dataNode : dataNodes) {
      CLUSTER.add(dataNode);
    }
    dataNodes[9].setDecommissioned();
    dataNodes[10].setDecommissioned();
  }

  /**
   * Test getting Data Center from given location.
   */
  @Test
  public void testGetDataCenter() {
    assertEquals("/d3",
        DFSNetworkTopologyWithDataCenter.getDataCenter("/d3/r2"));
  }

  /**
   * Test random choose
   */
  @Test
  public void testChooseRandom() {
    Node n;
    DatanodeDescriptor dd;
    HashSet<Node> excluded = new HashSet<>();
    n = CLUSTER.chooseRandomWithStorageTypeTwoTrial("/d1/r2", null,
        StorageType.ARCHIVE);
    // exclude host3
    excluded.add(n);

    // search with given scope being desired scope
    for (int i = 0; i<10; i++) {
      n = CLUSTER.chooseRandom("/d1/r2",excluded);
      assertTrue(n instanceof DatanodeDescriptor);
      dd = (DatanodeDescriptor) n;
      assertTrue(dd.getHostName().equals("host4") ||
          dd.getHostName().equals("host5"));
    }

    // search with given rack being exclude rack but in the same DC
    for (int i = 0; i<10; i++) {
      n = CLUSTER.chooseRandom("~/d1/r1",excluded);
      assertTrue(n instanceof DatanodeDescriptor);
      dd = (DatanodeDescriptor) n;
      assertTrue(dd.getHostName().equals("host4") ||
          dd.getHostName().equals("host5"));
    }

    // search with given DC being exclude DC
    for (int i = 0; i<10; i++) {
      n = CLUSTER.chooseRandom("~/d4",excluded);
      assertTrue(n instanceof DatanodeDescriptor);
      dd = (DatanodeDescriptor) n;
      assertTrue(DFSNetworkTopologyWithDataCenter
          .getDataCenter(dd.getNetworkLocation()).equals("/d1") ||
          DFSNetworkTopologyWithDataCenter
              .getDataCenter(dd.getNetworkLocation()).equals("/d2") ||
          DFSNetworkTopologyWithDataCenter
              .getDataCenter(dd.getNetworkLocation()).equals("/d3"));
    }

    // search with given DC
    for (int i = 0; i<10; i++) {
      n = CLUSTER.chooseRandom("/d2",excluded);
      assertTrue(n instanceof DatanodeDescriptor);
      dd = (DatanodeDescriptor) n;
      assertTrue(dd.getHostName().equals("host6") ||
          dd.getHostName().equals("host7")
          || dd.getHostName().equals("host8"));
    }
  }

  /**
   * Test the random choose with storage type.
   */
  @Test
  public void testChooseRandomWithStorageType() {
    Node n;
    DatanodeDescriptor dd;
    HashSet<Node> excluded = new HashSet<>();

    // search with given scope being desired scope
    for (int i = 0; i<10; i++) {
      n = CLUSTER.chooseRandomWithStorageType(
          "/d3/r4", null, StorageType.ARCHIVE);
      assertTrue(n instanceof DatanodeDescriptor);
      dd = (DatanodeDescriptor) n;
      assertTrue(dd.getHostName().equals("host12") ||
          dd.getHostName().equals("host13"));
    }

    // search with given scope being exclude scope

    // ~/d1/r1 should be find node in DC-d1 but exclude rack-r1
    // so if we exclude /d1/r1, if should be always either host4 or host5
    for (int i = 0; i<10; i++) {
      n = CLUSTER.chooseRandomWithStorageType(
          "~/d1/r1", null, StorageType.DISK);
      assertTrue(n instanceof DatanodeDescriptor);
      dd = (DatanodeDescriptor) n;
      assertTrue(dd.getHostName().equals("host4") ||
          dd.getHostName().equals("host5"));
    }

    // similar to above, except that we also exclude host10 here. so it should
    // always be host7
    n = CLUSTER.chooseRandomWithStorageType("/d3/r2", null, null,
        StorageType.RAM_DISK);
    dd = (DatanodeDescriptor) n;
    assertEquals("host10", dd.getHostName());
    // add host10 to exclude
    excluded.add(n);
    for (int i = 0; i<10; i++) {
      n = CLUSTER.chooseRandomWithStorageType(
          "~/d4", excluded, StorageType.RAM_DISK);
      assertTrue(n instanceof DatanodeDescriptor);
      dd = (DatanodeDescriptor) n;
      assertEquals("host7", dd.getHostName());
    }
  }

  /**
   * Test the random choose with storage type.
   */
  @Test
  public void testChooseRandomWithStorageTypeTwoTrial() {
    Node n;
    DatanodeDescriptor dd;
    HashSet<Node> excluded = new HashSet<>();

    // search with given scope being desired scope
    for (int i = 0; i<10; i++) {
      n = CLUSTER.chooseRandomWithStorageTypeTwoTrial(
          "/d3/r4", null, StorageType.ARCHIVE);
      assertTrue(n instanceof DatanodeDescriptor);
      dd = (DatanodeDescriptor) n;
      assertTrue(dd.getHostName().equals("host12") ||
          dd.getHostName().equals("host13"));
    }

    // search with given scope being exclude scope

    // ~/d1/r1 should be find node in DC-d1 but exclude rack-r1
    // so if we exclude /d1/r1, if should be always either host4 or host5
    for (int i = 0; i<10; i++) {
      n = CLUSTER.chooseRandomWithStorageTypeTwoTrial(
          "~/d1/r1", null, StorageType.DISK);
      assertTrue(n instanceof DatanodeDescriptor);
      dd = (DatanodeDescriptor) n;
      assertTrue(dd.getHostName().equals("host4") ||
          dd.getHostName().equals("host5"));
    }

    // similar to above, except that we also exclude host10 here. so it should
    // always be host7
    n = CLUSTER.chooseRandomWithStorageType("/d3/r2", null, null,
        StorageType.RAM_DISK);
    // add host10 to exclude
    excluded.add(n);
    for (int i = 0; i<10; i++) {
      n = CLUSTER.chooseRandomWithStorageTypeTwoTrial(
          "~/d4", excluded, StorageType.RAM_DISK);
      assertTrue(n instanceof DatanodeDescriptor);
      dd = (DatanodeDescriptor) n;
      assertEquals("host7", dd.getHostName());
    }
  }
}