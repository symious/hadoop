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
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_RPC_BLACKLIST_FILE;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_RPC_BLACKLIST_REFRESH_INTERVAL_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_RPC_BLACKLIST_REFRESH_INTERVAL_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestIPUsersBlacklist {

  private final GenericTestUtils.LogCapturer blacklistLog =
      GenericTestUtils.LogCapturer.captureLogs(IPUsersBlacklist.LOG);

  private final static String TEST_FILE1 = "testBlacklist1";
  private final static String TEST_FILE2 = "testBlacklist2";
  private final static String TEST_FILE_NOT_EXISTS = "testBlacklistNotExists";

  @Test
  public void testValidRefreshFileLogic() throws IOException,
      InterruptedException, TimeoutException {
    // Validate init refresh file.
    Configuration conf  = new Configuration();
    String blacklist = Objects.requireNonNull(TestIPUsersBlacklist.class.getClassLoader()
        .getResource(TEST_FILE1)).getPath();
    conf.set(HADOOP_SECURITY_RPC_BLACKLIST_FILE, blacklist);
    IPUsersBlacklist ipUsersBlackList = IPUsersBlacklist.getInstanceForTesting(conf);
    IPUsersBlacklist.BlacklistMetrics metrics = IPUsersBlacklist.metrics;
    assertEquals(ipUsersBlackList.getIpUsersCache().get().size(),  5);
    assertEquals(metrics.refreshSuccess.lastStat().numSamples(), 1);

    // Validate file not update, not to refresh.
    ipUsersBlackList.getIpUsersRefreshService().refreshFile();
    assertTrue(blacklistLog.getOutput().contains(blacklist +
        " no update, no need to refresh ip and users blacklist."));
    assertEquals(ipUsersBlackList.getIpUsersCache().get().size(),  5);
    blacklistLog.clearOutput();

    // Validate file update, automatic refresh.
    blacklist = Objects.requireNonNull(TestIPUsersBlacklist.class.getClassLoader()
        .getResource(TEST_FILE2)).getPath();
    ipUsersBlackList.setBlacklistFile(blacklist);
    ipUsersBlackList.setRefreshInterval(3000);
    // Sleep over the refresh interval.
    GenericTestUtils.waitFor(
        () -> ipUsersBlackList.getIpUsersCache().get().size() == 3,
        100, 5000);
    assertEquals(metrics.refreshSuccess.lastStat().numSamples(), 2);
    ipUsersBlackList.resetMetrics();
  }

  @Test
  public void testInvalidRefreshFileLogic() throws IOException {
    //Validate invalid refresh interval.
    Configuration conf  = new Configuration();
    conf.setLong(HADOOP_SECURITY_RPC_BLACKLIST_REFRESH_INTERVAL_KEY, -1);
    IPUsersBlacklist ipUsersBlacklist = IPUsersBlacklist.getInstanceForTesting(conf);
    assertTrue(blacklistLog.getOutput().contains("Invalid value -1 configured for " +
        "hadoop.security.rpc.blacklist.refresh.interval, should be greater than 0. " +
        "Using default: 300000 ms."));
    assertEquals(ipUsersBlacklist.getRefreshInterval(),
        HADOOP_SECURITY_RPC_BLACKLIST_REFRESH_INTERVAL_DEFAULT);
    blacklistLog.clearOutput();
    ipUsersBlacklist.resetMetrics();

    //Validate `hadoop.security.rpc.blacklist.file` not configures.
    conf.set(HADOOP_SECURITY_RPC_BLACKLIST_FILE, "");
    ipUsersBlacklist = IPUsersBlacklist.getInstanceForTesting(conf);
    IPUsersBlacklist.BlacklistMetrics metrics = IPUsersBlacklist.metrics;
    assertTrue(blacklistLog.getOutput().contains("Invalid value for config " +
        "hadoop.security.rpc.blacklist.file"));
    blacklistLog.clearOutput();
    assertEquals(metrics.refreshFailure.lastStat().numSamples(), 1);

    //Validate blacklist not not exists.
    ipUsersBlacklist.setBlacklistFile(TEST_FILE_NOT_EXISTS);
    ipUsersBlacklist.getIpUsersRefreshService().refreshFile();
    assertTrue(blacklistLog.getOutput().contains("Refresh ip and users blacklist failed " +
        TEST_FILE_NOT_EXISTS + " does not exist."));
    blacklistLog.clearOutput();
    assertEquals(metrics.refreshFailure.lastStat().numSamples(), 2);
    ipUsersBlacklist.resetMetrics();
  }

  @Test
  public void testCheckBlacklist()
      throws InterruptedException, TimeoutException, UnknownHostException {
    Configuration conf = new Configuration();
    String blacklist = Objects.requireNonNull(TestIPUsersBlacklist.class.getClassLoader()
        .getResource(TEST_FILE1)).getPath();
    conf.set(HADOOP_SECURITY_RPC_BLACKLIST_FILE, blacklist);
    IPUsersBlacklist ipUsersBlacklist = IPUsersBlacklist.getInstanceForTesting(conf);
    IPUsersBlacklist.BlacklistMetrics metrics = IPUsersBlacklist.metrics;
    assertEquals(ipUsersBlacklist.getIpUsersCache().get().size(), 5);
    assertEquals(metrics.refreshSuccess.lastStat().numSamples(), 1);

    // Validate user and ip not in blacklist.
    try {
      InetAddress ipAddress = InetAddress.getByName("10.101.1.200");
      ipUsersBlacklist.checkBlacklist(ipAddress, "userA");
      assertEquals(metrics.checkBlacklist.lastStat().numSamples(), 1);
    } catch (AuthenticationException e) {
      Assert.fail("Shouldn't reach here.");
    }

    // Validate user and ip in blacklist.
    try {
      InetAddress ipAddress = InetAddress.getByName("10.101.5.200");
      ipUsersBlacklist.checkBlacklist(ipAddress, "userA");
      assertEquals(metrics.checkBlacklist.lastStat().numSamples(), 2);
      Assert.fail("AuthenticationException expected.");
    } catch (AuthenticationException ioe) {
      // Ignore.
    }

    try {
      InetAddress ipAddress = InetAddress.getByName("10.101.1.200");
      ipUsersBlacklist.checkBlacklist(ipAddress, "user5");
      assertEquals(metrics.checkBlacklist.lastStat().numSamples(), 3);
      Assert.fail("AuthenticationException expected.");
    } catch (AuthenticationException ioe) {
      // Ignore.
    }

    try {
      InetAddress ipAddress = InetAddress.getByName("10.101.1.200");
      ipUsersBlacklist.checkBlacklist(ipAddress, "user3");
      assertEquals(metrics.checkBlacklist.lastStat().numSamples(), 4);
      Assert.fail("AuthenticationException expected.");
    } catch (AuthenticationException ioe) {
      // Ignore.
    }

    try {
      InetAddress ipAddress = InetAddress.getByName("10.101.6.200");
      ipUsersBlacklist.checkBlacklist(ipAddress, "user7");
      assertEquals(metrics.checkBlacklist.lastStat().numSamples(), 5);
      Assert.fail("AuthenticationException expected.");
    } catch (AuthenticationException ioe) {
      // Ignore.
    }

    // Validate file update, automatic refresh.
    blacklist = Objects.requireNonNull(TestIPUsersBlacklist.class.getClassLoader()
        .getResource(TEST_FILE2)).getPath();
    ipUsersBlacklist.setBlacklistFile(blacklist);
    ipUsersBlacklist.setRefreshInterval(3000);
    // Sleep over the refresh interval.
    GenericTestUtils.waitFor(
        () -> ipUsersBlacklist.getIpUsersCache().get().size() == 3,
        100, 5000);
    assertEquals(metrics.refreshSuccess.lastStat().numSamples(), 2);

    // Validate user and ip not in blacklist.
    try {
      InetAddress ipAddress = InetAddress.getByName("10.101.5.200");
      ipUsersBlacklist.checkBlacklist(ipAddress, "userA");
      assertEquals(metrics.checkBlacklist.lastStat().numSamples(), 6);
    } catch (AuthenticationException e) {
      Assert.fail("Shouldn't reach here.");
    }

    try {
      InetAddress ipAddress = InetAddress.getByName("10.101.6.200");
      ipUsersBlacklist.checkBlacklist(ipAddress, "user7");
      assertEquals(metrics.checkBlacklist.lastStat().numSamples(), 7);
    } catch (AuthenticationException ioe) {
      Assert.fail("Shouldn't reach here.");
    }
    ipUsersBlacklist.resetMetrics();
  }
}

