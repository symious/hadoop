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
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Test;

import java.util.ArrayList;
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
    JSONObject jsonObject = ZoneMoverKafkaTrigger.processMessage(standardHDFSAuditLog);
    assertEquals(jsonObject.get("allowed"), "true");
    assertEquals(jsonObject.get("src"), "/test/test.file");
    assertEquals(jsonObject.get("dst"), "/test/test.file.new");
  }

  @Test
  public void testCheckPaths() {
    List<Path> pathList1 = new ArrayList<>();
    List<Path> pathList2 = Arrays.asList(new Path("/test1"),
        new Path("/test2"), new Path("/test3"));
    ZoneMoverKafkaTrigger zoneMoverTrigger1 =
        new ZoneMoverKafkaTrigger(getConf(), pathList1);
    ZoneMoverKafkaTrigger zoneMoverTrigger2 =
        new ZoneMoverKafkaTrigger(getConf(), pathList2);
    String pathLoc1 = "/test2/test.file";
    String pathLoc2 = "/test4/test.file";
    assertFalse(zoneMoverTrigger1.checkPaths(pathLoc2));
    assertTrue(zoneMoverTrigger2.checkPaths(pathLoc1));
    assertFalse(zoneMoverTrigger2.checkPaths(pathLoc2));
  }
}