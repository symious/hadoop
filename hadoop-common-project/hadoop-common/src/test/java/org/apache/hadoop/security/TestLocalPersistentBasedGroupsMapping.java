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
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.hash.MD5FileUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestLocalPersistentBasedGroupsMapping {

  private final GenericTestUtils.LogCapturer mappingLog =
      GenericTestUtils.LogCapturer
          .captureLogs(LocalPersistentBasedGroupsMapping.LOG);

  private final static String TEST_FILE = "usergroups";
  private final static String TEST_FILE2 = "usergroups2";
  private final static String TEST_FILE_TEMP = "usergroups.temp";
  private final static String TEST_FILE_INVALID = "usergroups.invalid";

  /**
   * Initializes Mapping object, deletes checksum file if existed, executes
   * refreshMapping once to set isStartup to false
   *
   * @param localFile     File object of the local mapping file
   * @param cleanChecksum true to clean checksum before returning
   * @return a (true or spy copy of) Mapping object
   */
  private LocalPersistentBasedGroupsMapping getMapping(File localFile,
      boolean startService, boolean cleanChecksum) {
    LocalPersistentBasedGroupsMapping mapping;

    Configuration conf = new Configuration();
    conf.set(
        CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_IN_MEMORY_MAPPING_FILE_PATH_KEY,
        localFile.getAbsolutePath());
    conf.setBoolean(
        CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_IN_MEMORY_MAPPING_CHECKSUM_KEY,
        true);
    // No timeout for most tests
    conf.setTimeDuration(
        CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_IN_MEMORY_MAPPING_REFRESH_INTERVAL_KEY,
        42, TimeUnit.HOURS);

    mapping = Mockito
        .spy(LocalPersistentBasedGroupsMapping.getInstanceForTesting(conf));

    if (cleanChecksum) {
      File md5File = MD5FileUtils.getDigestFileForFile(localFile);
      if (md5File.exists()) {
        md5File.delete();
      }
    }

    if (startService) {
      mapping.initializeMappingRefreshService();
    }

    return mapping;
  }

  @Test
  public void testChecksumDoesNotExist() {
    ClassLoader classLoader = getClass().getClassLoader();
    final File testFile =
        new File(Objects.requireNonNull(classLoader.getResource(TEST_FILE)).getFile());
    LocalPersistentBasedGroupsMapping mapping =
        getMapping(testFile, true, true);

    // Second run should fail
    try {
      mapping.cacheGroupsRefresh();
      fail("Should fail during second refresh");
    } catch (IOException e) {
      assertTrue("Expected the logs to carry "
              + "a message about missing checksum not found but was: " + mappingLog
              .getOutput(),
          mappingLog.getOutput().contains("MD5 file does not exist"));
      mappingLog.clearOutput();
    }
  }

  @Test
  public void testChecksumMismatch() throws IOException {
    ClassLoader classLoader = getClass().getClassLoader();
    final File testFile =
        new File(Objects.requireNonNull(classLoader.getResource(TEST_FILE)).getFile());
    final File testFile2 =
        new File(Objects.requireNonNull(classLoader.getResource(TEST_FILE2)).getFile());
    LocalPersistentBasedGroupsMapping mapping =
        getMapping(testFile, true, true);

    // Save hash of testFile2 for testFile
    MD5Hash md5Hash = MD5FileUtils.computeMd5ForFile(testFile2);
    MD5FileUtils.saveMD5File(testFile, md5Hash);

    // Should fail due to mismatched checksum now
    try {
      mapping.cacheGroupsRefresh();
      fail("Should fail during second refresh due to mismatched checksum.");
    } catch (IOException e) {
      assertTrue("Expected the exception message to be about mismatched"
              + " checksum not found but was: " + mappingLog.getOutput(),
          e.getMessage().contains("First round checksum failed"));
      mappingLog.clearOutput();
    }
  }

  @Test
  public void testInvalidMappingFormat() {
    ClassLoader classLoader = getClass().getClassLoader();
    final File testFileInvalid =
        new File(
            Objects.requireNonNull(classLoader.getResource(TEST_FILE_INVALID)).getFile());
    LocalPersistentBasedGroupsMapping mapping =
        getMapping(testFileInvalid, false, true);

    mapping.initializeMappingRefreshService();
    assertTrue("Expected the exception message to be about invalid"
            + " mapping format but was: " + mappingLog.getOutput(),
        mappingLog.getOutput().contains("Unable to process mapping:"));
    mappingLog.clearOutput();
  }

  @Test
  public void testMappingLogic() throws IOException {
    ClassLoader classLoader = getClass().getClassLoader();
    final File testFile =
        new File(Objects.requireNonNull(classLoader.getResource(TEST_FILE)).getFile());
    LocalPersistentBasedGroupsMapping mapping =
        getMapping(testFile, true, true);

    MD5Hash md5Hash = MD5FileUtils.computeMd5ForFile(testFile);
    MD5FileUtils.saveMD5File(testFile, md5Hash);

    mapping.cacheGroupsRefresh();

    Assert.assertEquals(Arrays.asList("groupD", "groupC", "groupA"),
        mapping.getGroups("userA"));
    // UserB is commented out
    Assert.assertTrue(mapping.getGroups("userB").isEmpty());
    Assert.assertEquals(Arrays.asList("groupC", "groupA"),
        mapping.getGroups("userA1"));
    Assert.assertEquals(Arrays.asList("groupD", "groupA"),
        mapping.getGroups("userA2"));

  }

  @Test(timeout = 10000)
  public void testRefreshMapping() throws IOException, InterruptedException {
    ClassLoader classLoader = getClass().getClassLoader();
    final File testFile =
        new File(Objects.requireNonNull(classLoader.getResource(TEST_FILE)).getFile());
    final File testFile2 =
        new File(Objects.requireNonNull(classLoader.getResource(TEST_FILE2)).getFile());
    final File testFileTemp =
        new File(
            Objects.requireNonNull(classLoader.getResource(TEST_FILE_TEMP)).getFile());

    // Use TEST_FILE content first
    overwriteTestFileContent(testFile, testFileTemp, true);

    LocalPersistentBasedGroupsMapping mapping =
        getMapping(testFileTemp, true, false);

    Assert.assertEquals(Arrays.asList("groupD", "groupC", "groupA"),
        mapping.getGroups("userA"));
    Assert.assertTrue(mapping.getGroups("userB").isEmpty());
    Assert.assertEquals(Arrays.asList("groupC", "groupA"),
        mapping.getGroups("userA1"));
    Assert.assertEquals(Arrays.asList("groupD", "groupA"),
        mapping.getGroups("userA2"));
    Assert.assertTrue(mapping.getGroups("userB1").isEmpty());
    Assert.assertTrue(mapping.getGroups("userB2").isEmpty());

    // Use TEST_FILE2 content
    overwriteTestFileContent(testFile2, testFileTemp, true);
    MD5Hash md5Hash = MD5FileUtils.computeMd5ForFile(testFileTemp);
    MD5FileUtils.saveMD5File(testFileTemp, md5Hash);

    mapping.setRefreshInterval(3000);
    // Sleep over the refresh interval
    Thread.sleep(4000);

    // Call getGroups once to refresh in-memory mappings
    mapping.getGroups("userA");
    Assert.assertEquals(Arrays.asList("groupD", "groupC"),
        mapping.getGroups("userA"));
    Assert.assertEquals(Collections.singletonList("groupB"),
        mapping.getGroups("userB"));
    Assert.assertEquals(Collections.singletonList("groupE"),
        mapping.getGroups("userA1"));
    Assert.assertEquals(Collections.singletonList("groupE"),
        mapping.getGroups("userA2"));
    Assert.assertEquals(Arrays.asList("groupC", "groupB"),
        mapping.getGroups("userB1"));
    Assert.assertEquals(Arrays.asList("groupD", "groupB"),
        mapping.getGroups("userB2"));
  }

  @Test
  public void testMetrics() throws IOException {
    ClassLoader classLoader = getClass().getClassLoader();
    final File testFile =
        new File(Objects.requireNonNull(classLoader.getResource(TEST_FILE)).getFile());
    final File testFile2 =
        new File(Objects.requireNonNull(classLoader.getResource(TEST_FILE2)).getFile());
    final File testFileTemp =
        new File(
            Objects.requireNonNull(classLoader.getResource(TEST_FILE_TEMP)).getFile());
    final File testFileInvalid =
        new File(
            Objects.requireNonNull(classLoader.getResource(TEST_FILE_INVALID)).getFile());

    overwriteTestFileContent(testFile, testFileTemp, true);

    LocalPersistentBasedGroupsMapping mapping =
        getMapping(testFileTemp, false, false);
    LocalPersistentBasedGroupsMapping.resetMetrics();
    mapping.initializeMappingRefreshService();
    LocalPersistentBasedGroupsMapping.LocalGroupsMappingMetrics metrics =
        LocalPersistentBasedGroupsMapping.metrics;

    Assert.assertEquals(1, metrics.refreshTotal.value());
    Assert.assertEquals(0, metrics.forceRefreshTotal.value());
    Assert.assertEquals(3, metrics.userCount.value());
    Assert.assertEquals(3, metrics.groupCount.value());
    Assert.assertEquals(0, metrics.refreshFailuresTotal.value());
    Assert.assertEquals(0, metrics.mappingLineFailuresTotal.value());

    overwriteTestFileContent(testFileInvalid, testFileTemp, true);
    mapping.cacheGroupsRefresh();

    Assert.assertEquals(2, metrics.refreshTotal.value());
    Assert.assertEquals(1, metrics.forceRefreshTotal.value());
    Assert.assertEquals(2, metrics.userCount.value());
    Assert.assertEquals(1, metrics.groupCount.value());
    Assert.assertEquals(0, metrics.refreshFailuresTotal.value());
    Assert.assertEquals(2, metrics.mappingLineFailuresTotal.value());

    overwriteTestFileContent(testFile, testFileTemp, false);
    try {
      mapping.cacheGroupsRefresh();
    } catch (Exception ignored) {
      // Will fail due to mismatched checksum, ignore
    }

    Assert.assertEquals(3, metrics.refreshTotal.value());
    Assert.assertEquals(2, metrics.forceRefreshTotal.value());
    Assert.assertEquals(2, metrics.userCount.value());
    Assert.assertEquals(1, metrics.groupCount.value());
    Assert.assertEquals(1, metrics.refreshFailuresTotal.value());
    Assert.assertEquals(2, metrics.mappingLineFailuresTotal.value());

    overwriteTestFileContent(testFile2, testFileTemp, true);
    mapping.cacheGroupsRefresh();

    Assert.assertEquals(4, metrics.refreshTotal.value());
    Assert.assertEquals(3, metrics.forceRefreshTotal.value());
    Assert.assertEquals(6, metrics.userCount.value());
    Assert.assertEquals(4, metrics.groupCount.value());
    Assert.assertEquals(1, metrics.refreshFailuresTotal.value());
    Assert.assertEquals(2, metrics.mappingLineFailuresTotal.value());
  }

  private void overwriteTestFileContent(File src, File dst,
                                        boolean generateChecksum) throws IOException {
    InputStream in = new FileInputStream(src);
    OutputStream out = new FileOutputStream(dst);
    int n;
    while ((n = in.read()) != -1) {
      out.write(n);
    }
    if (generateChecksum) {
      MD5Hash md5Hash = MD5FileUtils.computeMd5ForFile(dst);
      MD5FileUtils.saveMD5File(dst, md5Hash);
    }
  }
}