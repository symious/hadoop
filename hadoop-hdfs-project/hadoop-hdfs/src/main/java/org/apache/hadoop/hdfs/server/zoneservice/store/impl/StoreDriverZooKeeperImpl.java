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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.zoneservice.store.BaseRecord;
import org.apache.hadoop.hdfs.server.zoneservice.store.MigrationRecord;
import org.apache.hadoop.hdfs.server.zoneservice.store.Query;
import org.apache.hadoop.hdfs.server.zoneservice.store.QueryResult;
import org.apache.hadoop.hdfs.server.zoneservice.store.StoreDriver;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.curator.ZKCuratorManager;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.util.curator.ZKCuratorManager.getNodePath;

public class StoreDriverZooKeeperImpl extends StoreDriver {
  /** Mark for slashes in path names. */
  protected static final String SLASH_MARK = "0SLASH0";
  /** Mark for colon in path names. */
  protected static final String COLON_MARK = "_";

  private static final Logger LOG =
      LoggerFactory.getLogger(StoreDriverZooKeeperImpl.class);

  // Configuration keys.
  public static final String ZONESERVICE_STORE_ZK_DRIVER_PREFIX =
      "dfs.zoneservice.driver.zk.";
  public static final String ZONESERVICE_STORE_ZK_PARENT_PATH =
      ZONESERVICE_STORE_ZK_DRIVER_PREFIX + "parent-path";
  public static final String ZONESERVICE_STORE_ZK_PARENT_PATH_DEFAULT =
      "/zoneservice-driver";

  // Directory to store the state store data.
  private String baseZNode;
  // Interface to ZooKeeper.
  private ZKCuratorManager zkManager;
  // ACLs for ZooKeeper.
  private List<ACL> zkAcl;

  @Override
  public boolean initDriver() {
    LOG.info("Initializing ZooKeeper connection");

    Configuration conf = getConf();
    baseZNode = conf.get(
        ZONESERVICE_STORE_ZK_PARENT_PATH,
        ZONESERVICE_STORE_ZK_PARENT_PATH_DEFAULT);
    try {
      this.zkManager = new ZKCuratorManager(conf);
      this.zkManager.start();
      this.zkAcl = ZKCuratorManager.getZKAcls(conf);
    } catch (IOException e) {
      LOG.error("Cannot initialize the ZK connection", e);
      return false;
    }

    // Initialize supported classes.
    if (!initRecordStorage(MigrationRecord.class)) {
      return false;
    };
    return true;
  }

  @Override
  public <T extends BaseRecord> boolean initRecordStorage(Class<T> clazz) {
    try {
      String checkPath = getNodePath(baseZNode, clazz.getName());
      zkManager.createRootDirRecursively(checkPath, zkAcl);
      return true;
    } catch (Exception e) {
      LOG.error("Cannot initialize ZK node for {}: {}",
          clazz.getName(), e.getMessage());
      return false;
    }
  }

  @Override
  public void close() throws Exception {
    if (zkManager  != null) {
      zkManager.close();
    }
  }

  private boolean writeNode(
      String znode, byte[] bytes, boolean update, boolean error) {
    try {
      boolean created = zkManager.create(znode);
      if (!update && !created && error) {
        LOG.info("Cannot write record \"{}\", it already exists", znode);
        return false;
      }

      // Write data
      zkManager.setData(znode, bytes, -1);
      return true;
    } catch (Exception e) {
      LOG.error("Cannot write record \"{}\": {}", znode, e.getMessage());
    }
    return false;
  }

  /**
   * Get the ZNode for a class.
   */
  private <T extends BaseRecord> String getZNodeForClass(Class<T> clazz) {
    return getNodePath(baseZNode, clazz.getName());
  }

  /**
   * Creates a record from a string returned by ZooKeeper.
   */
  private <T extends BaseRecord> T createRecord(
      String data, Stat stat, Class<T> clazz) throws IOException {
    T record = deserializeString(data, clazz);
    return record;
  }

  @Override
  public <T extends BaseRecord> T deserializeString(String data, Class<T> clazz) {
    return new GsonBuilder().create().fromJson(data, clazz);
  }

  @Override
  public <T extends BaseRecord> byte[] serialize(T record) {
    return new GsonBuilder().create().toJson(record).getBytes();
  }

  @Override
  public <T extends BaseRecord> T get(Query<T> query, Class<T> clazz) throws IOException {
    List<T> records = getMultiple(clazz, query);
    if (records.size() > 1) {
      throw new IOException("Found more than one object in collection");
    } else if (records.size() == 1) {
      return records.get(0);
    } else {
      return null;
    }
  }

  @Override
  public <T extends BaseRecord> QueryResult<T> getAll(Class<T> clazz) throws IOException {
    List<T> ret = new ArrayList<>();
    String znode = getZNodeForClass(clazz);
    try {
      List<String> children = zkManager.getChildren(znode);
      for (String child : children) {
        try {
          String path = getNodePath(znode, child);
          Stat stat = new Stat();
          String data = zkManager.getStringData(path, stat);
          boolean corrupted = false;
          if (data == null || data.equals("")) {
            // All records should have data, otherwise this is corrupted
            corrupted = true;
          } else {
            try {
              T record = createRecord(data, stat, clazz);
              ret.add(record);
            } catch (IOException e) {
              LOG.error("Cannot create record type \"{}\" from \"{}\": {}",
                  clazz.getSimpleName(), data, e.getMessage());
              corrupted = true;
            }
          }

          if (corrupted) {
            LOG.error("Cannot get data for {} at {}, cleaning corrupted data",
                child, path);
            zkManager.delete(path);
          }
        } catch (Exception e) {
          LOG.error("Cannot get data for {}: {}", child, e.getMessage());
        }
      }
    } catch (Exception e) {
      String msg = "Cannot get children for \"" + znode + "\": " +
          e.getMessage();
      LOG.error(msg);
      throw new IOException(msg);
    }
    return new QueryResult<T>(ret, Time.now());
  }

  @Override
  public <T extends BaseRecord> boolean put(T record, boolean allowUpdate, boolean errorIfExists)
      throws IOException {
    List<T> singletonList = new ArrayList<>();
    singletonList.add(record);
    return putAll(singletonList, allowUpdate, errorIfExists);
  }

  @Override
  public <T extends BaseRecord> boolean putAll(List<T> records, boolean allowUpdate,
      boolean errorIfExists) throws IOException {
    if (records.isEmpty()) {
      return true;
    }

    // All records should be the same
    T record0 = records.get(0);
    Class<? extends BaseRecord> recordClass = record0.getClass();
    String znode = getZNodeForClass(recordClass);

    boolean status = true;
    for (T record : records) {
      String primaryKey = getPrimaryKey(record);
      String recordZNode = getNodePath(znode, primaryKey);
      byte[] data = serialize(record);
      if (!writeNode(recordZNode, data, allowUpdate, errorIfExists)){
        status = false;
      }
    }
    return status;
  }
  @Override
  public <T extends BaseRecord> int remove(Query<T> query, Class<T> clazz) throws IOException {
    if (query == null) {
      return 0;
    }

    // Read the current data
    List<T> records = null;
    try {
      QueryResult<T> result = getAll(clazz);
      records = result.getRecords();
    } catch (IOException ex) {
      LOG.error("Cannot get existing records", ex);
      return 0;
    }

    // Check the records to remove
    String znode = getZNodeForClass(clazz);
    List<T> recordsToRemove = filterMultiple(query, records);

    // Remove the records
    int removed = 0;
    for (T existingRecord : recordsToRemove) {
      LOG.info("Removing \"{}\"", existingRecord);
      try {
        String primaryKey = getPrimaryKey(existingRecord);
        String path = getNodePath(znode, primaryKey);
        if (zkManager.delete(path)) {
          removed++;
        } else {
          LOG.error("Did not remove \"{}\"", existingRecord);
        }
      } catch (Exception e) {
        LOG.error("Cannot remove \"{}\"", existingRecord, e);
      }
    }
    return removed;
  }

  @Override
  public <T extends BaseRecord> boolean removeAll(Class<T> clazz) throws IOException {
    boolean status = true;
    String znode = getZNodeForClass(clazz);
    LOG.info("Deleting all children under {}", znode);
    try {
      List<String> children = zkManager.getChildren(znode);
      for (String child : children) {
        String path = getNodePath(znode, child);
        LOG.info("Deleting {}", path);
        zkManager.delete(path);
      }
    } catch (Exception e) {
      LOG.error("Cannot remove {}: {}", znode, e.getMessage());
      status = false;
    }
    return status;
  }

  public <T extends BaseRecord> List<T> getMultiple(
      Class<T> clazz, Query<T> query) throws IOException  {
    QueryResult<T> result = getAll(clazz);
    List<T> records = result.getRecords();
    List<T> ret = filterMultiple(query, records);
    return ret;
  }

  /**
   * Filters a list of records to find all records matching the query.
   */
  public static <T extends BaseRecord> List<T> filterMultiple(
      final Query<T> query, final Iterable<T> records) {

    List<T> matchingList = new ArrayList<>();
    for (T record : records) {
      if (query.matches(record)) {
        matchingList.add(record);
      }
    }
    return matchingList;
  }

  /**
   * Get the primary key for a record. If we don't want to store in folders, we
   * need to remove / from the name.
   */
  protected static String getPrimaryKey(BaseRecord record) {
    String primaryKey = record.getPrimaryKey();
    primaryKey = primaryKey.replaceAll("/", SLASH_MARK);
    primaryKey = primaryKey.replaceAll(":", COLON_MARK);
    return primaryKey;
  }
}