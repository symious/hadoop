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

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrCodec;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.XAttrHelper;
import org.apache.hadoop.hdfs.server.zoneservice.ReplicationRule;
import org.apache.hadoop.util.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class TestFSDirXAttrReplicationRuleOp {
  private static final ReplicationRule rule =
      ReplicationRule.parseFromString("/dc1:1,/dc2:2");
  private static final String invalidRule = "/dc1:a,/dc2:2";
  private static final PermissionStatus perm = new PermissionStatus(
      "hdfs", "supergroup",
      FsPermission.createImmutable((short) 0x1ff));
  private MiniDFSCluster cluster = null;

  private Configuration getConf() {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(CommonConfigurationKeys.IGNORE_SDI_AUTHENTICATE_KEY, true);
    return conf;
  }

  private XAttrFeature getFeature(String ruleString) throws IOException {
    XAttr.NameSpace namespace =
        DFSConfigKeys.DFS_ZONE_REPLICATION_RULE_XATTR_NAMESPACE;
    String name =
        DFSConfigKeys.DFS_ZONE_REPLICATION_RULE_XATTR_NAME;
    String attrKey =
        StringUtils.toLowerCase(namespace.name()) + "." + name;

    XAttr xattr = XAttrHelper.buildXAttr(
        attrKey, XAttrCodec.decodeValue(ruleString));
    List<XAttr> xAttrs = Lists.newArrayListWithCapacity(1);
    xAttrs.add(xattr);
    return XAttrStorage.createXAttrFeature(xAttrs);
  }

  @Test
  public void testGetRuleFromXAttr() throws IOException {
    INodeFile iNodeFile = new INodeFile(0L, null, perm, 0L, 0L, null,
        (short) 1, 128L, (byte)0);
    cluster = new MiniDFSCluster.Builder(getConf()).numDataNodes(1).build();
    FSDirectory fsd = new FSDirectory(cluster.getNamesystem(), getConf());
    Assert.assertNull(FSDirXAttrReplicationRuleOp.getRuleFromInodeFile(fsd, iNodeFile));

    iNodeFile.addXAttrFeature(getFeature(rule.toString()));
    Assert.assertEquals(rule, FSDirXAttrReplicationRuleOp.getRuleFromInodeFile(fsd, iNodeFile));

    iNodeFile = new INodeFile(0L, null, perm, 0L, 0L, null,
        (short) 1, 128L, (byte)0);
    iNodeFile.addXAttrFeature(getFeature(invalidRule));
    Assert.assertNull(FSDirXAttrReplicationRuleOp.getRuleFromInodeFile(fsd, iNodeFile));
  }

  @Test
  public void testHasRuleInXAttr() throws IOException {
    INodeFile iNodeFile = new INodeFile(0L, null, perm, 0L, 0L, null,
        (short) 1, 128L, (byte)0);
    cluster = new MiniDFSCluster.Builder(getConf()).numDataNodes(1).build();
    FSDirectory fsd = new FSDirectory(cluster.getNamesystem(), getConf());
    Assert.assertFalse(FSDirXAttrReplicationRuleOp.hasRuleInXAttr(fsd, iNodeFile));

    iNodeFile.addXAttrFeature(getFeature(rule.toString()));
    Assert.assertTrue(FSDirXAttrReplicationRuleOp.hasRuleInXAttr(fsd, iNodeFile));
  }
}
