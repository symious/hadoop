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
package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class TestShadowFilePasswordMapping {
  private static final Logger TESTLOG =
      LoggerFactory.getLogger(TestShadowFilePasswordMapping.class);

  private final GenericTestUtils.LogCapturer shellMappingLog =
      GenericTestUtils.LogCapturer.captureLogs(
          ShellBasedUnixGroupsMapping.LOG);

  private final static String TEST_SHADOW_FILE_1 = "shadow1";
  private final static String TEST_SHADOW_FILE_2 = "shadow2";

  @Test
  public void testCacheRefresh() throws IOException {
    ShadowFileRpcPasswordMapping mapping = new ShadowFileRpcPasswordMapping();
    Configuration conf = new Configuration();
    ClassLoader classLoader = getClass().getClassLoader();

    File shadowFile1 = new File(classLoader.getResource(TEST_SHADOW_FILE_1).getFile());
    conf.set(CommonConfigurationKeys.
        HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE, shadowFile1.getAbsolutePath());
    System.out.println(shadowFile1.getAbsolutePath());
    mapping.setConf(conf);

    mapping.cacheRefresh(true);

    assertEquals(mapping.getRpcPassword("a"), "aaaShadow1");
    assertNull(mapping.getRpcPassword("b"));
    assertEquals(mapping.getRpcPassword("c"), "cccShadow1");

    File shadowFile2 = new File(classLoader.getResource(TEST_SHADOW_FILE_2).getFile());
    conf.set(CommonConfigurationKeys.
        HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE, shadowFile2.getAbsolutePath());
    System.out.println(shadowFile1.getAbsolutePath());
    mapping.setConf(conf);

    mapping.cacheRefresh(true);

    assertEquals(mapping.getRpcPassword("a"), "aaaShadow2");
    assertNull(mapping.getRpcPassword("b"));
    assertEquals(mapping.getRpcPassword("c"), "cccShadow2");
  }
}


