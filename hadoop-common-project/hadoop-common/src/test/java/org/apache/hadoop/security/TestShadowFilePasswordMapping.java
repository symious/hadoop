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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.hash.MD5FileUtils;
import org.junit.Test;

import static org.apache.hadoop.security.PollingBasedFileWatcher.NAMENODE_SIGNAL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestShadowFilePasswordMapping {

  private final static String TEST_SHADOW_FILE_1 = "shadow1";
  private final static String TEST_SHADOW_FILE_2 = "shadow2";

  private void overwriteTestFileContent(File src, File dst) throws IOException {
    InputStream in = new FileInputStream(src);
    OutputStream out = new FileOutputStream(dst);
    int n;
    while ((n = in.read()) != -1) {
      out.write(n);
    }
    in.close();
    out.close();
  }

  @Test
  public void testCacheRefresh() throws IOException, InterruptedException, TimeoutException {
    Configuration conf = new Configuration();
    ClassLoader classLoader = getClass().getClassLoader();
    File tempFile = File.createTempFile(GenericTestUtils.getMethodName(), null);

    File shadowFile1 = new File(classLoader.getResource(TEST_SHADOW_FILE_1).getFile());
    overwriteTestFileContent(shadowFile1, tempFile);
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE,
        tempFile.getAbsolutePath());
    conf.setLong(CommonConfigurationKeys.HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE_CACHE_REFRESH_INTERVAL, 1);
    ShadowFileRpcPasswordMapping mapping = new ShadowFileRpcPasswordMapping();
    mapping.setConf(conf);

    mapping.cacheRefresh(true);

    assertEquals(mapping.getRpcPassword("a"), "aaaShadow1");
    assertNull(mapping.getRpcPassword("b"));
    assertEquals(mapping.getRpcPassword("c"), "cccShadow1");

    File shadowFile2 = new File(classLoader.getResource(TEST_SHADOW_FILE_2).getFile());
    overwriteTestFileContent(shadowFile2, tempFile);
    tempFile.setLastModified(Time.now() + 10000);
    mapping.cacheRefresh(true);

    assertEquals(mapping.getRpcPassword("a"), "aaaShadow2");
    assertNull(mapping.getRpcPassword("b"));
    assertEquals(mapping.getRpcPassword("c"), "cccShadow2");

    //Test cache refresh async
    overwriteTestFileContent(shadowFile1, tempFile);
    tempFile.setLastModified(Time.now() + 20000);
    GenericTestUtils.waitFor(() -> mapping.getRpcPassword("a").equals("aaaShadow1"), 100, 5000);

    assertNull(mapping.getRpcPassword("b"));
    assertEquals(mapping.getRpcPassword("c"), "cccShadow1");
  }

  @Test
  public void testChecksum() throws IOException {
    ClassLoader classLoader = getClass().getClassLoader();
    File shadowFile1 = new File(classLoader.getResource(TEST_SHADOW_FILE_1).getFile());
    File shadowFile2 = new File(classLoader.getResource(TEST_SHADOW_FILE_2).getFile());
    Configuration conf = new Configuration();
    conf.setBoolean(CommonConfigurationKeys.
        HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE_CHECKSUM_ENABLED, true);
    conf.setInt(CommonConfigurationKeys.
        HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE_CHECKSUM_MAX_ATTEMPTS, 1);
    conf.set(CommonConfigurationKeys.
        HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE, shadowFile1.getAbsolutePath());
    // Test using manual refreshing
    conf.setLong(
        CommonConfigurationKeys.HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE_CACHE_REFRESH_INTERVAL,
        100000000);
    ShadowFileRpcPasswordMapping mapping = new ShadowFileRpcPasswordMapping();
    mapping.setConf(conf);

    try {
      // Clean shadow1.md5
      File md5File = MD5FileUtils.getDigestFileForFile(shadowFile1);
      if (md5File.exists()) {
        MD5FileUtils.getDigestFileForFile(shadowFile1).delete();
      }
      // Ignore md5 when starting up
      // Start up sequence is already triggered by constructor

      // shadow1.md5 not exists
      try {
        mapping.cacheRefresh(true);
        fail("Should fail since md5 not exists.");
      } catch (Exception e) {
        assertTrue(e instanceof ShadowFileException);
      }


      // Create shadow1.md5
      MD5Hash md5Hash = MD5FileUtils.computeMd5ForFile(shadowFile1);
      MD5FileUtils.saveMD5File(shadowFile1, md5Hash);
      mapping.cacheRefresh(true);

      // Create shadow1.md5 based on shadow2
      MD5Hash md5Hash2 = MD5FileUtils.computeMd5ForFile(shadowFile2);
      MD5FileUtils.saveMD5File(shadowFile1, md5Hash2);
      try {
        mapping.cacheRefresh(true);
        fail("Should fail since md5 not match.");
      } catch (Exception e) {
        assertTrue(e instanceof ShadowFileException);
      }
    } finally {
      // Delete shadow1.md5
      MD5FileUtils.getDigestFileForFile(shadowFile1).delete();
    }
  }

  @Test
  public void testCacheNotStarted() {
    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeys.IPC_SERVER_RPC_CATEGORY_INTERNAL, NAMENODE_SIGNAL);
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE, "NOT_EXIST_FILE");
    assertThrows(RuntimeException.class, () -> new ShadowFileRpcPasswordMapping().setConf(conf));

    ClassLoader classLoader = getClass().getClassLoader();
    File shadowFile1 = new File(classLoader.getResource(TEST_SHADOW_FILE_1).getFile());
    conf.set(CommonConfigurationKeys.
        HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE, shadowFile1.getAbsolutePath());
    new ShadowFileRpcPasswordMapping().setConf(conf);
  }

  @Test
  public void testSlowChecksum() throws IOException, InterruptedException, TimeoutException {
    GenericTestUtils.LogCapturer mappingLog =
        GenericTestUtils.LogCapturer.captureLogs(ShadowFileRpcPasswordMapping.LOG);

    ClassLoader classLoader = getClass().getClassLoader();
    File tempFile = File.createTempFile(GenericTestUtils.getMethodName(), null);
    File shadowFile1 = new File(classLoader.getResource(TEST_SHADOW_FILE_1).getFile());
    overwriteTestFileContent(shadowFile1, tempFile);
    MD5Hash md5Hash = MD5FileUtils.computeMd5ForFile(tempFile);
    MD5FileUtils.saveMD5File(tempFile, md5Hash);

    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE,
        tempFile.getAbsolutePath());
    conf.setLong(
        CommonConfigurationKeys.HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE_CACHE_REFRESH_INTERVAL, 1);
    conf.setBoolean(
        CommonConfigurationKeys.HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE_CHECKSUM_ENABLED, true);
    ShadowFileRpcPasswordMapping mapping = new ShadowFileRpcPasswordMapping();
    mapping.setConf(conf);
    mapping.cacheRefresh(true);

    assertEquals(mapping.getRpcPassword("a"), "aaaShadow1");

    File shadowFile2 = new File(classLoader.getResource(TEST_SHADOW_FILE_2).getFile());
    overwriteTestFileContent(shadowFile2, tempFile);
    tempFile.setLastModified(Time.now() + 10000);

    GenericTestUtils.waitFor(() -> mappingLog.getOutput().contains("Checksum failed on attempt"),
        100, 2000);
    md5Hash = MD5FileUtils.computeMd5ForFile(tempFile);
    MD5FileUtils.saveMD5File(tempFile, md5Hash);

    GenericTestUtils.waitFor(() -> mapping.getRpcPassword("a").equals("aaaShadow2"), 100, 2000);
  }
}
