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
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class TestReplicationRuleUtil {
  private MiniDFSCluster cluster = null;
  private static final long FILE_LEN = 1024;
  private static final short REPLICATION = 1;
  private static final Path path = new Path("/test.txt");
  private static final ReplicationRule rule =
      ReplicationRule.parseFromString("/dc1:1,/dc2:2");

  private Configuration getConf() {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(CommonConfigurationKeys.IGNORE_SDI_AUTHENTICATE_KEY, true);
    return conf;
  }

  @Test
  public void testGetRuleFromXAttr() throws IOException {
    cluster = new MiniDFSCluster.Builder(getConf()).numDataNodes(1).build();
    DistributedFileSystem fs = cluster.getFileSystem();
    DFSTestUtil.createFile(fs, path, FILE_LEN, REPLICATION, 0L);

    ReplicationRuleUtil util = new ReplicationRuleUtil(fs);
    try {
      util.getRuleFromXAttr(path);
      Assert.fail("The last operation should throw exception!");
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains(
          "At least one of the attributes provided was not found."));
    }

    util.setRuleToXAttr(path, rule);
    Assert.assertEquals(rule, util.getRuleFromXAttr(path));
  }

  @Test
  public void testHasRuleInXAttr() throws IOException {
    cluster = new MiniDFSCluster.Builder(getConf()).numDataNodes(1).build();
    DistributedFileSystem fs = cluster.getFileSystem();
    DFSTestUtil.createFile(fs, path, FILE_LEN, REPLICATION, 0L);

    ReplicationRuleUtil util = new ReplicationRuleUtil(fs);
    Assert.assertFalse(util.hasRuleInXAttr(path));
    util.setRuleToXAttr(path, rule);
    Assert.assertTrue(util.hasRuleInXAttr(path));
  }
}