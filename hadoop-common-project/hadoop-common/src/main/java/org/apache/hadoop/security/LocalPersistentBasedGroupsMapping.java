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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.hash.MD5FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A local cache based implementation of {@link GroupMappingServiceProvider}
 * that periodically reads a persistent copy of users' group memberships into
 * a memory cache.
 */
@InterfaceAudience.LimitedPrivate({ "HDFS", "MapReduce" })
@InterfaceStability.Evolving
public class LocalPersistentBasedGroupsMapping extends PollingBasedFileWatcher
    implements GroupMappingServiceProvider {

  @VisibleForTesting
  protected static final Logger LOG =
      LoggerFactory.getLogger(LocalPersistentBasedGroupsMapping.class);

  private boolean useChecksum;
  private static final List<String> EMPTY_GROUPS = new LinkedList<>();

  private final AtomicReference<ConcurrentHashMap<String, List<String>>>
      mappingCache = new AtomicReference<>();
  private volatile boolean isStartup = true;

  // Metrics
  protected static LocalGroupsMappingMetrics metrics =
      LocalGroupsMappingMetrics.create();

  @VisibleForTesting
  protected static void resetMetrics() {
    metrics = LocalGroupsMappingMetrics.create();
  }

  LocalPersistentBasedGroupsMapping() {
  }

  @Override
  public boolean onModified() {
    try {
      refreshMapping(false);
      return true;
    } catch (IOException e) {
      LOG.error("In-memory usergroup mapping refresh failed.", e);
      return false;
    }
  }

  @Metrics(about = "Local Groups mapping refresh metrics", context = "localGroups")
  static class LocalGroupsMappingMetrics {
    final MetricsRegistry registry =
        new MetricsRegistry("LocalGroupsMappingMetrics");

    @Metric("Rate of successful in-memory mapping refresh and latency (ms)")
    MutableRate refreshSuccess;
    @Metric("Rate of failed in-memory mapping refresh and latency (ms)")
    MutableRate refreshFailure;
    @Metric("Total refreshes since startup")
    MutableGaugeLong refreshTotal;
    @Metric("Empty refreshes since startup")
    MutableGaugeLong emptyRefreshTotal;
    @Metric("Total force refreshes since startup")
    MutableGaugeLong forceRefreshTotal;
    @Metric("Refresh failures since startup")
    MutableGaugeLong refreshFailuresTotal;
    @Metric("Mapping line failures since startup")
    MutableGaugeLong mappingLineFailuresTotal;
    @Metric("User count")
    MutableGaugeLong userCount;
    @Metric("Group count")
    MutableGaugeLong groupCount;

    static LocalGroupsMappingMetrics create() {
      return DefaultMetricsSystem.instance()
          .register(new LocalGroupsMappingMetrics());
    }
  }

  synchronized protected void refreshMapping(boolean force)
      throws IOException {
    long start = Time.now();
    if (force) {
      metrics.forceRefreshTotal.incr();
    }
    metrics.refreshTotal.incr();

    MD5Hash md5Hash = null;
    if (useChecksum && !isStartup) {
      md5Hash = checksum();
      if (md5Hash == null) {
        refreshFailure("First round checksum failed.", start);
      }
    }

    ConcurrentHashMap<String, List<String>> groupUsers =
        new ConcurrentHashMap<>();
    ConcurrentHashMap<String, List<String>> localMappings =
        new ConcurrentHashMap<>();
    BufferedReader br = null;

    try {
      FileInputStream file = new FileInputStream(trackFile);
      Reader fr = new InputStreamReader(file, StandardCharsets.UTF_8);
      br = new BufferedReader(fr);
      String line;
      while ((line = br.readLine()) != null) {
        try {
          processLine(groupUsers, line);
        } catch (EmptyLocalMappingException e) {
          metrics.mappingLineFailuresTotal.incr();
          LOG.debug("Empty group: " + line, start);
        } catch (IllegalLocalMappingException e) {
          metrics.mappingLineFailuresTotal.incr();
          LOG.warn("Unable to process mapping: " + line, start);
        }
      }
    } finally {
      if (br != null) {
        br.close();
      }
    }

    convertGroupUsersToUserGroups(groupUsers, localMappings);

    if (useChecksum && !isStartup) {
      MD5Hash fileHash = MD5FileUtils.computeMd5ForFile(this.file);
      if (md5Hash != null && !md5Hash.equals(fileHash)) {
        refreshFailure("Local mappings changed during renewal.", start);
      }
    }

    if (localMappings.isEmpty()) {
      metrics.emptyRefreshTotal.incr();
      refreshFailure("Usergroup mappings are empty. Keeping old mappings.", start);
    }

    mappingCache.set(localMappings);
    metrics.refreshSuccess.add(Time.now() - start);
    metrics.userCount.set(localMappings.size());
    metrics.groupCount.set(groupUsers.size());
    LOG.info("Loaded {} user-groups from local mappings", localMappings.size());
    if (isStartup) {
      isStartup = false;
    }
  }

  private void refreshFailure(String msg, long start)
      throws LocalMappingException {
    metrics.refreshFailure.add(Time.now() - start);
    metrics.refreshFailuresTotal.incr();
    throw new LocalMappingException(msg);
  }

  /**
   * Convert mappings of groups -> users into mappings of users -> groups
   *
   * @param groupUsers    map of groups:users
   * @param localMappings map of users:groups
   */
  protected void convertGroupUsersToUserGroups(
      ConcurrentHashMap<String, List<String>> groupUsers,
      ConcurrentHashMap<String, List<String>> localMappings) {
    for (Map.Entry<String, List<String>> entry : groupUsers.entrySet()) {
      for (String user : entry.getValue()) {
        if (!localMappings.containsKey(user)) {
          localMappings.put(user, new ArrayList<>());
        }
        localMappings.get(user).add(entry.getKey());
      }
    }
  }

  private void processLine(ConcurrentHashMap<String, List<String>> groupUsers,
      String line) throws LocalMappingException {
    if (line.startsWith("#"))
      return;
    String[] colonSplit = line.split(":");
    if (colonSplit.length == 1) {
      throw new EmptyLocalMappingException(line);
    }
    if (colonSplit.length != 2) {
      throw new IllegalLocalMappingException(line);
    }
    String group = colonSplit[0];
    String[] users = colonSplit[1].split(",");
    groupUsers.put(group, Arrays.asList(users));
  }

  private MD5Hash checksum() {
    MD5Hash result = null;
    try {
      MD5Hash fileHash = MD5FileUtils.computeMd5ForFile(file);
      MD5Hash storedHash = MD5FileUtils.readStoredMd5ForFile(file);
      if (storedHash == null) {
        LOG.error("MD5 file does not exist: " + MD5FileUtils.getDigestFileForFile(file));
      }
      if (!fileHash.equals(storedHash)) {
        return null;
      }
      result = fileHash;
    } catch (IOException e) {
      LOG.error("Error checksum: " + e.getMessage());
    }
    return result;
  }

  @Override
  synchronized public void setConf(Configuration conf) {
    if (conf != null) {
      super.setConf(conf);
      updateParams(
          conf.get(CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_IN_MEMORY_MAPPING_FILE_PATH_KEY,
              CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_IN_MEMORY_MAPPING_FILE_PATH_DEFAULT),
          conf.getTimeDuration(
              CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_IN_MEMORY_MAPPING_REFRESH_INTERVAL_KEY,
              CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_IN_MEMORY_MAPPING_REFRESH_INTERVAL_DEFAULT,
              TimeUnit.MILLISECONDS),
          conf.getTimeDuration(
              CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_IN_MEMORY_MAPPING_FORCE_REFRESH_INTERVAL_KEY,
              CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_IN_MEMORY_MAPPING_FORCE_REFRESH_INTERVAL_DEFAULT,
              TimeUnit.MILLISECONDS));
      useChecksum = conf.getBoolean(
          CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_IN_MEMORY_MAPPING_CHECKSUM_KEY,
          CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_IN_MEMORY_MAPPING_CHECKSUM_DEFAULT);
    }
  }

  private static class LocalMappingException extends IOException {
    public LocalMappingException(String message) {
      super(message);
    }

    @Override
    public String toString() {
      return "LocalMappingException " + super.getMessage();
    }
  }

  private static class IllegalLocalMappingException
      extends LocalMappingException {
    public IllegalLocalMappingException(String message) {
      super(message);
    }
  }

  private static class EmptyLocalMappingException
      extends LocalMappingException {
    public EmptyLocalMappingException(String message) {
      super(message);
    }
  }

  @Override
  public List<String> getGroups(String user) throws IOException {
    if (!mappingCache.get().containsKey(user)) {
      return EMPTY_GROUPS;
    }
    return mappingCache.get().get(user);
  }

  @Override
  public void cacheGroupsRefresh() throws IOException {
    refreshMapping(true);
  }

  @Override
  public void cacheGroupsAdd(List<String> groups) throws IOException {
    // does nothing in this provider of user to groups mapping
  }
}