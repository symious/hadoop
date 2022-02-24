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
import java.util.List;

/**
 * Operations for a driver to manage records in the Store.
 */
public interface RecordOperations {
  /**
   * Get a single record from the store that matches the query.
   */
  <T extends BaseRecord> T get(Query<T> query, Class<T> clazz) throws IOException;

  /**
   * Get a list of records from the store that likes the query.
   */
  <T extends BaseRecord> List<T> getLike(Query<T> query, Class<T> clazz) throws IOException;

  /**
   * Get all records from the store.
   */
  <T extends BaseRecord> QueryResult<T> getAll(Class<T> clazz) throws IOException;

  /**
   * Put a single record to the store.
   */
  <T extends BaseRecord> boolean put(
      T record, boolean allowUpdate, boolean errorIfExists) throws IOException;

  /**
   * Put all given records to the store.
   */
  <T extends BaseRecord> boolean putAll(
      List<T> records, boolean allowUpdate, boolean errorIfExists)
      throws IOException;

  /**
   * Remove one record from the store that matches the query.
   */
  <T extends BaseRecord> int remove(Query<T> query, Class<T>clazz) throws IOException;

  /**
   * Remove all records from the store.
   */
  <T extends BaseRecord> boolean removeAll(Class<T> clazz) throws IOException;
}
