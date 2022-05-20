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

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class StoreDriver implements RecordOperations{
  private static final Logger LOG =
      LoggerFactory.getLogger(StoreDriver.class);

  /** State Store configuration. */
  private Configuration conf;

  /** Identifier for the driver. */
  private String identifier;

  /**
   * Initialize the store connection.
   */
  public boolean init(final Configuration config, String id) {
    this.conf = config;
    this.identifier = id;
    boolean success = initDriver();
    if (!success) {
      LOG.error("Cannot initialize driver for {}", getDriverName());
      return false;
    }
    return true;
  }

  /**
   * Initialize storage for a single record class.
   */
  public abstract <T extends BaseRecord> boolean initRecordStorage(Class<T> clazz);

  /**
   * Get the State Store configuration.
   */
  protected Configuration getConf() {
    return this.conf;
  }

  /**
   * Get the identifier.
   */
  public String getIdentifier() {
    return this.identifier;
  }

  /**
   * Prepare the driver to access data storage.
   */
  public abstract boolean initDriver();

  /**
   * Close the State Store driver connection.
   */
  public abstract void close() throws Exception;

  /**
   * Get the name of the driver implementation for debugging.
   */
  private String getDriverName() {
    return this.getClass().getSimpleName();
  }

  /**
   * Deserialize record.
   */
  public abstract <T extends BaseRecord> T deserializeString(String data, Class<T> clazz);

  /**
   * Serialize record.
   */
  public abstract <T extends BaseRecord> byte[] serialize(T record);
}