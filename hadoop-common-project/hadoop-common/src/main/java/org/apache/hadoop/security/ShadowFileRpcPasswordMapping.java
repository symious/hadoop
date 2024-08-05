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
import java.util.concurrent.ConcurrentHashMap;
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
 * A simple shadow file based implementation of
 * {@link RpcPasswordMappingServiceProvider}.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class ShadowFileRpcPasswordMapping extends PollingBasedFileWatcher
    implements RpcPasswordMappingServiceProvider {

  @VisibleForTesting
  protected static final Logger LOG =
      LoggerFactory.getLogger(ShadowFileRpcPasswordMapping.class);

  /** Metrics to track shadow file activity */
  static ShadowFileMetrics metrics = ShadowFileMetrics.create();
  private boolean checksumEnabled;
  private int maxChecksumAttempts;

  private volatile boolean isStartup = true;

  private final AtomicReference<ConcurrentHashMap<String, RpcPasswordAndBypass>>
      cacheRef = new AtomicReference<>();

  public ShadowFileRpcPasswordMapping() {
  }

  @Override
  public boolean onModified() {
    try {
      cacheRefresh(false);
      return true;
    } catch (IOException e) {
      LOG.error("RPC password mapping refresh failed.", e);
      return false;
    }
  }

  /**
   * ShadowFileMetrics maintains shadow file related statistics.
   */
  @Metrics(about="shadow file related metrics", context="shadowFile")
  static class ShadowFileMetrics {
    final MetricsRegistry registry = new MetricsRegistry("ShadowFileMetrics");

    @Metric("Rate of successful shadow file refresh and latency (milliseconds)")
    private MutableRate refreshSuccess;
    @Metric("Rate of failed shadow file refresh and latency (milliseconds)")
    private MutableRate refreshFailure;
    @Metric("Total force refreshes since startup")
    private MutableGaugeLong forceRefreshTotal;
    @Metric("Refresh total since startup")
    private MutableGaugeLong refreshTotal;
    @Metric("Refresh failures since startup")
    private MutableGaugeLong refreshFailuresTotal;
    @Metric("Process line failure since startup")
    private MutableGaugeLong processLineFailuresTotal;
    @Metric("Count of users")
    private MutableGaugeLong usersCount;

    static ShadowFileMetrics create() {
      return DefaultMetricsSystem.instance().register(new ShadowFileMetrics());
    }
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (conf != null) {
      checksumEnabled = conf.getBoolean(
          CommonConfigurationKeys.HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE_CHECKSUM_ENABLED,
          CommonConfigurationKeys.HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE_CHECKSUM_ENABLED_DEFAULT);
      maxChecksumAttempts = conf.getInt(
          CommonConfigurationKeys.HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE_CHECKSUM_MAX_ATTEMPTS,
          CommonConfigurationKeys.HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE_CHECKSUM_MAX_ATTEMPTS_DEFAULT);
      if (maxChecksumAttempts < 1) {
        LOG.info("Non negative max checksum attempts {}, checksum disabled.", maxChecksumAttempts);
        checksumEnabled = false;
      }
      updateParams(conf.get(CommonConfigurationKeys.HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE,
              CommonConfigurationKeys.HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE_DEFAULT),
          conf.getLong(
              CommonConfigurationKeys.HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE_CACHE_REFRESH_INTERVAL,
              CommonConfigurationKeys.HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE_CACHE_REFRESH_INTERVAL_DEFAULT)
              * 1000,
          conf.getLong(CommonConfigurationKeys.HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE_CACHE_SEC,
              CommonConfigurationKeys.HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE_CACHE_SEC_DEFAULT)
              * 1000);
    }
  }

  /**
   * Returns rpcPassword for a user.
   *
   * @param userName get rpcPassword for this user
   * @return list of rpcPassword for a given user
   */
  @Override
  public String getRpcPassword(String userName) {
    try {
      if (cacheRef.get() == null || cacheRef.get().isEmpty()) {
        cacheRefresh(true);
      }
    } catch (IOException e) {
      LOG.error("Failed to refresh cache!", e);
    }
    if (!cacheRef.get().containsKey(userName)) {
      return null;
    }
    return cacheRef.get().get(userName).getRpcPassword();
  }

  @Override
  public boolean isBypassUser(String user) {
    try {
      if (cacheRef.get() == null || cacheRef.get().isEmpty()) {
        cacheRefresh(true);
      }
    } catch (IOException e) {
      LOG.error("Failed to refresh cache!", e);
    }
    if (!cacheRef.get().containsKey(user)) {
      return false;
    }
    return cacheRef.get().get(user).isBypass();
  }

  @Override
  public synchronized void cacheRefresh(boolean force) throws IOException {
    long start = Time.now();
    if (force) {
      metrics.forceRefreshTotal.incr();
    }
    metrics.refreshTotal.incr();
    BufferedReader br = null;
    ConcurrentHashMap<String, RpcPasswordAndBypass> updateCache =
        new ConcurrentHashMap<>();

    MD5Hash md5Hash = null;
    if (checksumEnabled && !isStartup) {
      // Try to match checksum up to 3 times, 1 second between each attempt
      for (int attempt = 0; attempt < maxChecksumAttempts; attempt++) {
        md5Hash = checksum();
        if (md5Hash != null || attempt == maxChecksumAttempts - 1) {
          break;
        }
        LOG.info("Checksum failed on attempt {}/3, retrying...", attempt + 1);
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      if (md5Hash == null) {
        refreshFailure("First round checksum not match.", start);
      }
    }

    try {
      FileInputStream file = new FileInputStream(trackFile);
      Reader fr = new InputStreamReader(file, StandardCharsets.UTF_8);
      br = new BufferedReader(fr);
      String line;
      while ((line = br.readLine()) != null) {
        //process the line
        try {
          processRow(updateCache, line);
        } catch (IllegalShadowLineException e) {
          metrics.processLineFailuresTotal.incr();
          LOG.error("Unable to process shadow line: " + line, start);
        }
      }
    } finally {
      if (br != null) {
        br.close();
      }
    }

    if (checksumEnabled && !isStartup) {
      MD5Hash fileHash = MD5FileUtils.computeMd5ForFile(file);
      if (md5Hash != null && !md5Hash.equals(fileHash)) {
        refreshFailure("Second round checksum not match", start);
      }
    }
    if (updateCache.isEmpty()) {
      refreshFailure("New shadowFile is empty", start);
    }
    cacheRef.set(updateCache);
    metrics.refreshSuccess.add(Time.now() - start);
    metrics.usersCount.set(updateCache.size());
    LOG.info("Refreshed {} records from shadowFile.", updateCache.size());
    if (isStartup) {
      isStartup = false;
    }
  }

  private MD5Hash checksum() {
    MD5Hash fileHash, storedHash;
    MD5Hash result = null;
    try {
      fileHash = MD5FileUtils.computeMd5ForFile(file);
      storedHash = MD5FileUtils.readStoredMd5ForFile(file);
      if (storedHash == null) {
        LOG.error("MD5 File not exists: " + MD5FileUtils.getDigestFileForFile(file));
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

  private void processRow(
      ConcurrentHashMap<String, RpcPasswordAndBypass> cache,
      String string) throws IllegalShadowLineException {
    // handle comment line
    if (string.startsWith("#")) {
      return;
    }

    String[] commaSplit = string.split(",");
    if (commaSplit.length != 3) {
      throw new IllegalShadowLineException(string);
    }
    String user = commaSplit[0];
    String shadow = commaSplit[1];
    boolean bypass = commaSplit[2].equalsIgnoreCase("true");
    cache.put(user, new RpcPasswordAndBypass(shadow, bypass));
  }

  private static class IllegalShadowLineException extends IOException {
    public IllegalShadowLineException(String message) {
      super(message);
    }

    @Override
    public String toString() {
      return "IllegalShadowLineException " + super.getMessage();
    }
  }

  private void refreshFailure(String reason, long start)
      throws ShadowFileException {
    metrics.refreshFailure.add(Time.now() - start);
    metrics.refreshFailuresTotal.incr();
    throw new ShadowFileException(reason);
  }
}
