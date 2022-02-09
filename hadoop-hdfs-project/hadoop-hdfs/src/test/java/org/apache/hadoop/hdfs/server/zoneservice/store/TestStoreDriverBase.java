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
package org.apache.hadoop.hdfs.server.zoneservice.store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestStoreDriverBase {


  public <T extends BaseRecord> void testPut(StoreDriver driver, Class<T> clazz)
      throws IllegalArgumentException, ReflectiveOperationException,
      IOException, SecurityException {
    // Clear all records.
    driver.removeAll(clazz);
    QueryResult<T> records = driver.getAll(clazz);
    assertTrue(records.getRecords().isEmpty());

    // Put multiple
    List<T> insertList = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      T newRecord = generateFakeRecord(clazz);
      insertList.add(newRecord);
    }
    // Verify
    assertTrue(driver.putAll(insertList, false, true));
    records = driver.getAll(clazz);
    assertEquals(10, records.getRecords().size());

    // Put single
    T singleRecord = generateFakeRecord(clazz);
    driver.put(singleRecord, false, true);
    // Verify
    records = driver.getAll(clazz);
    assertEquals(11, records.getRecords().size());

    // Put single which already exists, but it isn't allowed to update.
    boolean status = driver.put(singleRecord, false, true);
    // Verify
    records = driver.getAll(clazz);
    assertEquals(11, records.getRecords().size());
    assertFalse(status);

    // Put single which already exists, but it is allowed to update.
    status = driver.put(singleRecord, true, false);
    // Verify
    assertTrue(status);
    records = driver.getAll(clazz);
    assertEquals(11, records.getRecords().size());
  }

  public <T extends BaseRecord> void testRemove(StoreDriver driver, Class<T> clazz)
      throws IllegalArgumentException, IllegalAccessException, IOException {
    // Remove all
    assertTrue(driver.removeAll(clazz));
    QueryResult<T> records = driver.getAll(clazz);
    assertTrue(records.getRecords().isEmpty());

    // Put multiple
    List<T> insertList = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      T newRecord = generateFakeRecord(clazz);
      insertList.add(newRecord);
    }
    // Verify
    assertTrue(driver.putAll(insertList, false, true));
    records = driver.getAll(clazz);
    assertEquals(records.getRecords().size(), 10);

    // Remove Single
    Query<T> query = new Query(records.getRecords().get(0));
    assertEquals(1, driver.remove(query,clazz));
    // Verify
    records = driver.getAll(clazz);
    assertEquals(records.getRecords().size(), 9);

    // Remove all
    assertTrue(driver.removeAll(clazz));
    // Verify
    records = driver.getAll(clazz);
    assertTrue(records.getRecords().isEmpty());
  }

  public <T extends BaseRecord> void testGet(StoreDriver driver, Class<T> clazz) throws IllegalAccessException, IOException {
    // Fetch empty list
    driver.removeAll(clazz);
    QueryResult<T> result = driver.getAll(clazz);
    assertNotNull(result);
    List<T> records = result.getRecords();
    assertEquals(records.size(), 0);

    // Put multiple
    List<T> insertList = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      T newRecord = generateFakeRecord(clazz);
      insertList.add(newRecord);
    }
    // Verify
    assertTrue(driver.putAll(insertList, false, true));
    result = driver.getAll(clazz);
    assertEquals(result.getRecords().size(), 10);

    // Get single
    Query<T> query = new Query(result.getRecords().get(0));
    // Verify
    T r0 = driver.get(query, clazz);
    assertTrue(r0.equals(result.getRecords().get(0)));

    // Get multiple
    QueryResult<T> result2 = driver.getAll(clazz);
    // Verify
    assertEquals(10, result2.getRecords().size());
    for (int i = 0; i < 10; ++i) {
      assertEquals(result.getRecords().get(i), result2.getRecords().get(i));
    }
  }

  private <T extends BaseRecord> T generateFakeRecord(Class<T> recordClass)
      throws IllegalArgumentException {
    if (recordClass.equals(MigrationRecord.class)) {
      return (T)new MigrationRecord(generateRandomString(), generateRandomString(), generateRandomString());
    }
    return null;
  }

  private String generateRandomString() {
    String randomString = "randomString-" + new Random().nextInt();
    return randomString;
  }
}
