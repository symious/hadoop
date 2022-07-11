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
package org.apache.hadoop.net;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.NET_TOPOLOGY_IP_RANGE_DC_MAPPING_FILE_KEY;
import static org.junit.Assert.assertEquals;

public class TestIpRangeScriptBasedMapping {

  private final String ipRange1 = "1.2.0.0/16";
  private final String ipRange2 = "5.6.7.0/24";

  private final String hostName1 = "1.2.3.4";
  private final String hostName2 = "5.6.7.8";
  private final String hostName3 = "9.10.11.12";

  @Test
  public void testResolve() throws IOException {
    File ipRange2DCFile = File.createTempFile(getClass().getSimpleName() +
        ".testResolve", ".txt");
    Files.write(ipRange1 + ",dc1\n" +
        ipRange2 + ",dc2\n", ipRange2DCFile, Charsets.UTF_8);
    ipRange2DCFile.deleteOnExit();
    IpRangeScriptBasedMapping mapping = new IpRangeScriptBasedMapping();
    Configuration conf = new Configuration();
    conf.set(NET_TOPOLOGY_IP_RANGE_DC_MAPPING_FILE_KEY, ipRange2DCFile.getCanonicalPath());
    mapping.setConf(conf);

    List<String> names = new ArrayList<String>();
    names.add(hostName1);
    names.add(hostName2);
    names.add(hostName3);

    List<String> result = mapping.resolve(names);
    assertEquals(names.size(), result.size());
    assertEquals("/dc1", result.get(0));
    assertEquals("/dc2", result.get(1));
    assertEquals(NetworkTopology.DEFAULT_RACK, result.get(2));
  }

  @Test
  public void testTableCaching() throws IOException {
    File ipRange2DCFile = File.createTempFile(getClass().getSimpleName() +
        ".testResolve", ".txt");
    Files.write(ipRange1 + ",dc1\n" +
        ipRange2 + ",dc2\n", ipRange2DCFile, Charsets.UTF_8);
    ipRange2DCFile.deleteOnExit();
    IpRangeScriptBasedMapping mapping = new IpRangeScriptBasedMapping();
    Configuration conf = new Configuration();
    conf.set(NET_TOPOLOGY_IP_RANGE_DC_MAPPING_FILE_KEY, ipRange2DCFile.getCanonicalPath());
    mapping.setConf(conf);

    List<String> names = new ArrayList<String>();
    names.add(hostName1);
    names.add(hostName2);

    List<String> result1 = mapping.resolve(names);
    assertEquals(names.size(), result1.size());
    assertEquals("/dc1", result1.get(0));
    assertEquals("/dc2", result1.get(1));

    // unset the file, see if it gets read again
    conf.set(NET_TOPOLOGY_IP_RANGE_DC_MAPPING_FILE_KEY, "some bad value for a file");

    List<String> result2 = mapping.resolve(names);
    assertEquals(result1, result2);
  }

  @Test
  public void testNoFile() {
    TableMapping mapping = new TableMapping();

    Configuration conf = new Configuration();
    mapping.setConf(conf);

    List<String> names = new ArrayList<String>();
    names.add(hostName1);
    names.add(hostName2);

    List<String> result = mapping.resolve(names);
    assertEquals(names.size(), result.size());
    assertEquals(NetworkTopology.DEFAULT_RACK, result.get(0));
    assertEquals(NetworkTopology.DEFAULT_RACK, result.get(1));
  }

  @Test
  public void testFileDoesNotExist() {
    TableMapping mapping = new TableMapping();

    Configuration conf = new Configuration();
    conf.set(NET_TOPOLOGY_IP_RANGE_DC_MAPPING_FILE_KEY, "/this/file/does/not/exist");
    mapping.setConf(conf);

    List<String> names = new ArrayList<String>();
    names.add(hostName1);
    names.add(hostName2);

    List<String> result = mapping.resolve(names);
    assertEquals(names.size(), result.size());
    assertEquals(result.get(0), NetworkTopology.DEFAULT_RACK);
    assertEquals(result.get(1), NetworkTopology.DEFAULT_RACK);
  }

  @Test
  public void testReloadCachedMappings() throws IOException {
    File ipRange2DCFile = File.createTempFile(getClass().getSimpleName() +
        ".testResolve", ".txt");
    Files.write(ipRange1 + ",dc1\n" +
        ipRange2 + ",dc2\n", ipRange2DCFile, Charsets.UTF_8);
    ipRange2DCFile.deleteOnExit();

    IpRangeScriptBasedMapping mapping = new IpRangeScriptBasedMapping();

    Configuration conf = new Configuration();
    conf.set(NET_TOPOLOGY_IP_RANGE_DC_MAPPING_FILE_KEY, ipRange2DCFile.getCanonicalPath());
    mapping.setConf(conf);

    List<String> names = new ArrayList<String>();
    names.add(hostName1);
    names.add(hostName2);
    names.add(hostName3);

    List<String> result = mapping.resolve(names);
    assertEquals(names.size(), result.size());
    assertEquals("/dc1", result.get(0));
    assertEquals("/dc2", result.get(1));
    assertEquals(NetworkTopology.DEFAULT_RACK, result.get(2));

    ipRange2DCFile = File.createTempFile(getClass().getSimpleName() +
        ".testResolve", ".txt");
    String ipRange3 = "9.10.0.0/16";
    Files.write(ipRange1 + ",dc3\n" +
        ipRange2 + ",dc4\n" + ipRange3 + ",dc5\n",
        ipRange2DCFile, Charsets.UTF_8);
    ipRange2DCFile.deleteOnExit();
    conf.set(NET_TOPOLOGY_IP_RANGE_DC_MAPPING_FILE_KEY, ipRange2DCFile.getCanonicalPath());

    mapping.reloadIpRange2DC(conf);

    names = new ArrayList<String>();
    names.add(hostName1);
    names.add(hostName2);
    names.add(hostName3);

    result = mapping.resolve(names);
    assertEquals(names.size(), result.size());
    assertEquals("/dc3", result.get(0));
    assertEquals("/dc4", result.get(1));
    // Tips: hostName3 be cached by ScriptBasedMapping,
    // so it will not be reloaded by IpRangeScriptBasedMapping.
    assertEquals(NetworkTopology.DEFAULT_RACK, result.get(2));
  }

  @Test(timeout=60000)
  public void testBadFile() throws IOException {
    File ipRange2DCFile = File.createTempFile(getClass().getSimpleName() +
        ".testResolve", ".txt");
    String badIpRange1 = "1.2.0.0/hello";
    String badIpRange2 = "5.6.7.0\\24";
    String badIpRange3 = "9.10.00.0.0.0/24";
    String badIpRange4 = "hello bad case";
    Files.write(badIpRange1 + "\n" + badIpRange2 + "\n"
        + badIpRange3 + "\n" + badIpRange4 + "\n",
        ipRange2DCFile, Charsets.UTF_8);
    ipRange2DCFile.deleteOnExit();

    IpRangeScriptBasedMapping mapping = new IpRangeScriptBasedMapping();

    Configuration conf = new Configuration();
    conf.set(NET_TOPOLOGY_IP_RANGE_DC_MAPPING_FILE_KEY, ipRange2DCFile.getCanonicalPath());
    mapping.setConf(conf);

    List<String> names = new ArrayList<String>();
    names.add(hostName1);
    names.add(hostName2);
    names.add(hostName3);

    List<String> result = mapping.resolve(names);
    assertEquals(names.size(), result.size());
    assertEquals(NetworkTopology.DEFAULT_RACK, result.get(0));
    assertEquals(NetworkTopology.DEFAULT_RACK, result.get(1));
    assertEquals(NetworkTopology.DEFAULT_RACK, result.get(2));
  }
}
