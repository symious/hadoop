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
package org.apache.hadoop.hdfs.server.zoneservice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.balancer.NameNodeConnector;
import org.junit.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestZoneChecker {
  private static final long FILE_LEN = 1024;
  private static final short REPLICATION = 3;

  @Test
  public void testGetReplicaInfo() throws Exception {
    final String[] hosts1 = {"host0", "host1", "host2"};
    final String[] racks1 = {"/dc0/rack0", "/dc0/rack0", "/dc0/rack1"};

    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(CommonConfigurationKeys.IGNORE_SDI_AUTHENTICATE_KEY, true);
    MiniDFSCluster cluster = new MiniDFSCluster
        .Builder(conf)
        .numDataNodes(hosts1.length).hosts(hosts1).racks(racks1).build();
    cluster.waitActive();
    DistributedFileSystem fs = cluster.getFileSystem();
    String dirName = "/test_zch/";
    String pathName = dirName + "testGetReplicaInfo1.txt";
    fs.mkdir(new Path(dirName), new FsPermission("777"));

    // write two files
    Path path1 = new Path(pathName);
    // client(127.0.0.1) will be mapped to a random node in (host0, host1, host2)
    DFSTestUtil.createFile(fs, path1, FILE_LEN, REPLICATION, 0L);
    DFSTestUtil.createFile(fs, new Path(dirName + "testGetReplicaInfo2.txt"),
        FILE_LEN, REPLICATION, 0L);

    NameNodeConnector nnc;
    Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
    nnc = new NameNodeConnector(namenodes.iterator().next(),
        Collections.singletonList(new Path(pathName)), conf, 1);
    final ZoneChecker zch = new ZoneChecker(nnc, conf);

    //Check the zone check for file
    Map<String, Short> replicaInfoMap = new HashMap<>();
    replicaInfoMap.put("/dc0", (short) 3);
    ReplicationRule replicationRule =
        ReplicationRule.parseFromMap(replicaInfoMap);
    Map<ReplicationRule, List<String>> replicationRuleListMap =
        new HashMap<>();
    replicationRuleListMap.put(replicationRule, new ArrayList<>(
        Collections.singletonList(pathName)));
    assertEquals(replicationRuleListMap, zch.getReplicaInfo(pathName));

    //Check the zone check for dir
    replicationRuleListMap.clear();
    replicationRuleListMap.put(replicationRule, new ArrayList<>(
        Collections.singletonList(dirName)));
    assertEquals(replicationRuleListMap, zch.getReplicaInfo(dirName));
  }
}