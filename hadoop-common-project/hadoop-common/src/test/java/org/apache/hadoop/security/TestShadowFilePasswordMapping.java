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
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestShadowFilePasswordMapping {

  private final static String TEST_SHADOW_FILE_1 = "shadow1";
  private final static String TEST_SHADOW_FILE_2 = "shadow2";

  @Test
  public void testCacheRefresh() throws IOException, InterruptedException {
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
    System.out.println(shadowFile2.getAbsolutePath());
    mapping.setConf(conf);

    mapping.cacheRefresh(true);

    assertEquals(mapping.getRpcPassword("a"), "aaaShadow2");
    assertNull(mapping.getRpcPassword("b"));
    assertEquals(mapping.getRpcPassword("c"), "cccShadow2");

    //Test cache refresh async
    conf.set(CommonConfigurationKeys.
        HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE, shadowFile1.getAbsolutePath());
    conf.setBoolean(CommonConfigurationKeys.HADOOP_SECURITY_RPC_PASSWORD_CACHE_REFRESH_ASYNC, true);
    conf.setLong(CommonConfigurationKeys.HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE_CACHE_REFRESH_INTERVAL, 8);
    System.out.println(shadowFile1.getAbsolutePath());
    mapping.setConf(conf);
    mapping.start();

    Thread.sleep(900);

    assertEquals(mapping.getRpcPassword("a"), "aaaShadow1");
    assertNull(mapping.getRpcPassword("b"));
    assertEquals(mapping.getRpcPassword("c"), "cccShadow1");
  }
}


