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
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestZoneMoverKafkaTrigger {
  private Configuration getConf() {
    Configuration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_ZONEMOVER_KAFKA_USERNAME, "test");
    conf.set(DFSConfigKeys.DFS_ZONEMOVER_KAFKA_PASSWORD, "test");
    conf.set(DFSConfigKeys.DFS_ZONEMOVER_KAFKA_BOOTSTRAP_SERVERS,
        "localhost:9093");
    conf.set(DFSConfigKeys.DFS_ZONEMOVER_KAFKA_TOPIC, "test");
    conf.set(DFSConfigKeys.DFS_ZONEMOVER_KAFKA_GROUP_ID, "test");
    conf.setBoolean(CommonConfigurationKeys.IGNORE_SDI_AUTHENTICATE_KEY, true);
    return conf;
  }

  @Test
  public void testProcessMessage() throws JSONException {
    final String standardHDFSAuditLog = "2000-01-01 00:30:35,067 INFO " +
        "FSNamesystem.audit: allowed=true\tugi=A (auth:SIMPLE)\t" +
        "ip=/10.10.10.10\tcmd=rename\tsrc=" +
        "/test/test.file\t" +
        "dst=/test/test.file.new\t" +
        "perm=A:Agroup:rwxrwx---\tproto=rpc";
    JSONObject jsonObject = ZoneMoverKafkaTrigger.message2json(standardHDFSAuditLog);
    assertEquals(jsonObject.get("allowed"), "true");
    assertEquals(jsonObject.get("src"), "/test/test.file");
    assertEquals(jsonObject.get("dst"), "/test/test.file.new");

    final String specialHDFSAuditLog = "2024-01-08 14:30:59,215 INFO FSNamesystem.audit: " +
        "allowed=true\tugi=hdfs_test (auth:SIMPLE)\tip=/10.10.20.100\tcmd=complete\t" +
        "src=/projects/feature_src=online_labeled_data/project=item_rebate/topic=checkout/_SUCCESS\t" +
        "dst=2564424474\tperm=null\tproto=rpc\tcallerContext=clientIp:10.20.50.90," +
        "clientPort:54178,clientId:c39147d6c58d456f827567109e498e15,clientCallId:265";
    JSONObject jsonObject1 = ZoneMoverKafkaTrigger.message2json(specialHDFSAuditLog);
    assertEquals(jsonObject1.get("allowed"), "true");
    assertEquals(jsonObject1.get("src"),
        "/projects/feature_src=online_labeled_data/project=item_rebate/topic=checkout/_SUCCESS");
    assertEquals(jsonObject1.get("dst"), "2564424474");
  }

  @Test
  public void testCheckPaths() throws IOException {
    final String[] racks = {"/dc0/rack0", "/dc1/rack1", "/dc1/rack2"};
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster
        .Builder(conf)
        .numDataNodes(racks.length).racks(racks).build();
    URI namenode = DFSUtil.createUri(HdfsConstants.HDFS_URI_SCHEME,
        cluster.getNameNode().getNameNodeAddress());
    List<Path> pathList = Arrays.asList(new Path("/test1"),
        new Path("/test2"), new Path("/test3"));
    ZoneMoverKafkaTrigger zoneMoverTrigger =
        new ZoneMoverKafkaTrigger(getConf(), pathList, namenode);
    String pathLoc1 = "/test2/test.file";
    String pathLoc2 = "/test4/test.file";
    assertTrue(zoneMoverTrigger.checkPaths(pathLoc1));
    assertFalse(zoneMoverTrigger.checkPaths(pathLoc2));

  }

  @Test
  public void testContainKeyWords() {
    Configuration conf = new Configuration();
    conf.set(DFSConfigKeys.DFS_ZONEMOVER_TRIGGER_SKIP_COMPLETE_KEYWORDS_KEY,
        "spark-staging, _temporary, .hoodie");

    ZoneMoverKafkaTrigger zoneMoverTrigger =
        new ZoneMoverKafkaTrigger(getConf(), null, URI.create("hdfs://test/"));

    assertTrue(zoneMoverTrigger.containKeyWords(
        ".spark-staging-857f6bac-cc51-4ad6-927b-aaef215fa714/_temporary",
        conf.getTrimmedStringCollection(
            DFSConfigKeys.DFS_ZONEMOVER_TRIGGER_SKIP_COMPLETE_KEYWORDS_KEY))
    );
  }
}