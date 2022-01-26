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
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.NoSuchElementException;

public class TestZoneReplicationCoordinator {

  private MiniDFSCluster cluster = null;

  private Configuration getConf() {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(CommonConfigurationKeys.IGNORE_SDI_AUTHENTICATE_KEY, true);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 1L);
    return conf;
  }

  @Test
  public void testWaitReplication() throws IOException {
    // construct a cluster with 3 datanodes
    Configuration conf = getConf();
    final String[] racks = {"/dc0/rack0", "/dc0/rack1", "/dc0/rack2"};
    final String[] hosts = {"host0", "host1", "host2"};
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).
        racks(racks).hosts(hosts).build();

    // create a file with replication = 3
    DistributedFileSystem fs = cluster.getFileSystem();
    String filePath = "/test.txt";
    DFSTestUtil.createFile(fs, new Path(filePath), 1024L, (short)1, 0L);

    // check replications if the cluster has enough datanodes
    ZoneReplicationCoordinator coordinator = new ZoneReplicationCoordinator(conf, fs);
    fs.setReplication(new Path(filePath), (short)3);
    coordinator.addFile(filePath, 2, 1);
    long timeout = 6000L;
    ZoneReplicationCoordinator.FileState fileState = coordinator.getNextFinishedFile(timeout);
    Assert.assertNotNull(fileState);
    Assert.assertEquals(filePath, fileState.getFilePath());

    // keep waiting if the cluster does not have valid
    // datanodes to store new replicas
    fs.setReplication(new Path(filePath), (short)4);
    coordinator.addFile(filePath, 1, 1);
    fileState = coordinator.getNextFinishedFile(timeout);
    Assert.assertNull(fileState);

    // add one more datanode to store the new replica
    final String[] racks2 = {"/dc0/rack3"};
    final String[] hosts2 = {"host3"};
    cluster.startDataNodes(conf, hosts2.length, true, null, racks2, hosts2, null, false);
    // wait some time to do replication and block report
    fileState = coordinator.getNextFinishedFile(timeout * 2);
    Assert.assertNotNull(fileState);
    Assert.assertEquals(filePath, fileState.getFilePath());

    coordinator.waitForCheckCompletion();
    try {
      coordinator.getNextFinishedFile(timeout);
      Assert.fail("Should encounter NoSuchElementException!");
    } catch (NoSuchElementException e) {
      // no need to handle
    }
  }

  @After
  public void teardown() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }
}
