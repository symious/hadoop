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
package org.apache.hadoop.hdfs.server.zoneservice.store.impl;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.server.zoneservice.store.BaseRecord;
import org.apache.hadoop.hdfs.server.zoneservice.store.MigrationRecord;
import org.apache.hadoop.hdfs.server.zoneservice.store.Query;
import org.apache.hadoop.hdfs.server.zoneservice.store.StoreDriver;
import org.apache.hadoop.hdfs.server.zoneservice.store.TestStoreDriverBase;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ZONESERVICE_STORE_DRIVER_CLASS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestStoreDriverZooKeeperImpl extends TestStoreDriverBase {

  private static StoreDriver driver;

  @BeforeClass
  public static void setupCluster() throws Exception {
    TestingServer curatorTestingServer = new TestingServer();
    curatorTestingServer.start();
    String connectString = curatorTestingServer.getConnectString();
    CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
        .connectString(connectString)
        .retryPolicy(new RetryNTimes(100, 100))
        .build();
    curatorFramework.start();

    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeys.ZK_ADDRESS, connectString);

    // Create the ZK State Store
    Class<? extends StoreDriver> driverClass = conf.getClass(
        DFS_ZONESERVICE_STORE_DRIVER_CLASS,
        StoreDriverZooKeeperImpl.class,
        StoreDriver.class);
    driver = ReflectionUtils.newInstance(driverClass, conf);
    driver.init(conf, "test-driver");
  }

  @AfterClass
  public static void tearDownCluster() throws Exception {
    if (driver != null) {
      driver.close();
    }
  }

  @Test
  public void testGet() throws Exception {
    testGet(driver, MigrationRecord.class);
  }

  @Test
  public void testPut()
      throws IllegalArgumentException, ReflectiveOperationException, IOException {
    testPut(driver, MigrationRecord.class);
  }

  @Test
  public void testRemove()
      throws IllegalArgumentException, ReflectiveOperationException,
      IOException, SecurityException {
    testRemove(driver, MigrationRecord.class);
  }

  @Test
  public void testSerialize() {
    BaseRecord record = new MigrationRecord("ns0", "/test/path", "dc0:2,dc1:1");
    byte[] data = driver.serialize(record);
    BaseRecord deserializeRecord =
        driver.deserializeString(new String(data), MigrationRecord.class);
    assertEquals(record, deserializeRecord);
  }

  @Test
  public void testMigrationRecord() throws Exception {
    // Validate no set client idc.
    MigrationRecord record = new MigrationRecord("ns0", "/test/path", "dc0:2,dc1:1");
    MigrationRecord existedRecord =
        driver.get(new Query<>(record), MigrationRecord.class);
    assertNull(existedRecord);
    assertTrue(driver.put(record, true, false));

    existedRecord =
        driver.get(new Query<>(record), MigrationRecord.class);
    assertEquals(record, existedRecord);
    assertEquals("", existedRecord.getClientIDC());

    // Validate set cross idc, to update exist record.
    record = new MigrationRecord("ns0", "/test/path", "dc0:2,dc1:1", "monitor",
        "/dc3");
    assertTrue(driver.put(record, true, false));
    existedRecord =
        driver.get(new Query<>(record), MigrationRecord.class);
    assertEquals(record, existedRecord);
    assertEquals("/dc3", existedRecord.getClientIDC());

    // Validate no set cross idc , to update exist record.
    record = new MigrationRecord("ns0", "/test/path", "dc0:2,dc1:1", "monitor");
    assertTrue(driver.put(record, true, false));
    existedRecord =
        driver.get(new Query<>(record), MigrationRecord.class);
    assertEquals(record, existedRecord);
    assertEquals("", existedRecord.getClientIDC());
  }
}