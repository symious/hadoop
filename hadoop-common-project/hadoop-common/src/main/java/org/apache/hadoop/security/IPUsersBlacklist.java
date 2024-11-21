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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.hadoop.util.hash.MD5FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_RPC_BLACKLIST_FILE;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_RPC_BLACKLIST_REFRESH_INTERVAL_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_RPC_BLACKLIST_REFRESH_INTERVAL_KEY;

/**
 * The IPUsersBlacklist class represents a list of IPs and users that are banned from accessing.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class IPUsersBlacklist {
  public static final Logger LOG = LoggerFactory.getLogger(IPUsersBlacklist.class.getName());

  private final AtomicReference<ConcurrentHashMap<BlacklistIP, Set<String>>> ipUsersCache =
      new AtomicReference<>();

  private final static String COMMENT_BEGIN_CHAR = "#";
  private static IPUsersBlacklist ipUsersBlacklist = null;
  private MD5Hash lastMd5Hash = null;
  private String blacklistFile;
  private long refreshInterval;
  private volatile boolean isStopped = false;

  private final ScheduledExecutorService scheduledExecutor =
      HadoopExecutors.newSingleThreadScheduledExecutor(
          new ThreadFactoryBuilder().setDaemon(true).
              setNameFormat("IPUsersBlacklistRefresh").build());
  private ScheduledFuture<?> refreshTask;
  private IPUsersRefreshService ipUsersRefreshService;

  private IPUsersBlacklist(Configuration conf) {
    blacklistFile = conf.get(HADOOP_SECURITY_RPC_BLACKLIST_FILE);
    refreshInterval = conf.getTimeDuration(HADOOP_SECURITY_RPC_BLACKLIST_REFRESH_INTERVAL_KEY,
        HADOOP_SECURITY_RPC_BLACKLIST_REFRESH_INTERVAL_DEFAULT, TimeUnit.MILLISECONDS);
    if (refreshInterval <= 0) {
      LOG.warn("Invalid value {} configured for hadoop.security.rpc.blacklist.refresh.interval, " +
          "should be greater than 0. Using default: {} ms.", refreshInterval,
          HADOOP_SECURITY_RPC_BLACKLIST_REFRESH_INTERVAL_DEFAULT);
      refreshInterval = HADOOP_SECURITY_RPC_BLACKLIST_REFRESH_INTERVAL_DEFAULT;
    }

    ipUsersRefreshService = new IPUsersRefreshService();
    ipUsersRefreshService.refreshFile();
    refreshTask = scheduledExecutor.scheduleWithFixedDelay(
        ipUsersRefreshService, refreshInterval, refreshInterval, TimeUnit.MILLISECONDS);
    LOG.info("Initialized ip and users blacklist refresh service with refresh interval: {} ms " +
            "and blacklistFile: {}.", refreshInterval, blacklistFile);
  }

  public static synchronized IPUsersBlacklist getIPUsersBlacklistService(Configuration conf) {
    if (ipUsersBlacklist == null) {
      LOG.info("Create new IPUsersBlacklist object");
      ipUsersBlacklist = new IPUsersBlacklist(conf);
    }
    return ipUsersBlacklist;
  }

  public static synchronized void shutdown() {
    if (ipUsersBlacklist != null) {
      ipUsersBlacklist.stop();
      ipUsersBlacklist = null;
      LOG.info("Clear IPUsersBlacklist object");
    }
  }

  // Metrics
  protected static BlacklistMetrics metrics = BlacklistMetrics.create();

  /**
   * BlacklistMetrics maintains ip and users blacklist file related statistics.
   */
  @Metrics(about = "IP Users Blacklist refresh metrics", context = "blacklist")
  static class BlacklistMetrics {
    final MetricsRegistry registry =
        new MetricsRegistry("BlacklistMetrics");

    @Metric("Rate of successful refresh and latency (ms)")
    MutableRate refreshSuccess;
    @Metric("Rate of failed refresh and latency (ms)")
    MutableRate refreshFailure;
    @Metric("Rate of check blacklist and latency (ms)")
    MutableRate checkBlacklist;

    static BlacklistMetrics create() {
      return DefaultMetricsSystem.instance().register(new BlacklistMetrics());
    }
  }

  /**
   * Stops the refresh service.
   */
  private synchronized void stop() {
    if (!isStopped) {
      isStopped = true;
      if (refreshTask != null) {
        refreshTask.cancel(true);
      }
      scheduledExecutor.shutdown();
      try {
        if (!scheduledExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
          scheduledExecutor.shutdownNow();
        }
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while shutting down scheduledExecutor.", e);
        scheduledExecutor.shutdownNow();
        Thread.currentThread().interrupt();
      }
      LOG.info("Stopped IPUsersBlacklist refresh service.");
    }
  }

  class IPUsersRefreshService implements Runnable {
    @Override
    public void run() {
      if (isStopped) {
        LOG.warn("IPUsersRefreshService is stopped.");
        return;
      }
      refreshFile();
    }

    protected synchronized void refreshFile() {
      long start = Time.now();
      if (blacklistFile == null || blacklistFile.isEmpty()) {
        refreshFailure(start, String.format("Invalid value for config %s.",
            HADOOP_SECURITY_RPC_BLACKLIST_FILE));
        return;
      }

      File file = new File(blacklistFile);
      if (!file.exists()) {
        refreshFailure(start, String.format("%s does not exist.", blacklistFile));
        return;
      }

      MD5Hash md5Hash = getMD5Hash(blacklistFile);
      if (md5Hash == null) {
        refreshFailure(start, String.format("%s get MD5Hash failed.", blacklistFile));
        return;
      }

      if (md5Hash.equals(lastMd5Hash)) {
        LOG.info("{} no update, no need to refresh ip and users blacklist.", blacklistFile);
        return;
      }

      ConcurrentHashMap<BlacklistIP, Set<String>> newIpUsers = new ConcurrentHashMap<>();

      try (BufferedReader reader = new BufferedReader(new InputStreamReader(
          Files.newInputStream(file.toPath()), StandardCharsets.UTF_8))) {
        String line;
        while ((line = reader.readLine()) != null) {
          try {
            processLine(newIpUsers, line);
          } catch (Exception e) {
            LOG.warn("Unable to process line: {} ", line);
          }
        }
      } catch (IOException e) {
        refreshFailure(start, e.getMessage());
        return;
      }
      lastMd5Hash = md5Hash;
      ipUsersCache.set(newIpUsers);
      long elapsed = Time.now() - start;
      metrics.refreshSuccess.add(elapsed);
      LOG.info("Loaded {} ip-users from {} cost {} ms.", newIpUsers.size(), blacklistFile, elapsed);
    }

    private void refreshFailure(long start, String msg) {
      LOG.warn("Refresh ip and users blacklist failed {}", msg);
      metrics.refreshFailure.add(Time.now() - start);
    }

    private void processLine(ConcurrentHashMap<BlacklistIP, Set<String>> ipUsers, String line)
        throws IllegalArgumentException {
      line = line.trim();
      if (line.isEmpty() || line.startsWith(COMMENT_BEGIN_CHAR)) {
        return;
      }
      String[] ipUser = line.split("=");
      if (ipUser.length != 2) {
        throw new IllegalArgumentException(line);
      }
      String ip = ipUser[0].trim();
      String users = ipUser[1];
      if (ip.equals("*") && users.equals("*")) {
        throw new IllegalArgumentException(line);
      }

      Object[] key = parseIpPattern(ip);
      BlacklistIP blacklistIP = new BlacklistIP(ip, key);
      Set<String> existUsers = ipUsers.get(blacklistIP);
      Set<String> newUsers = new HashSet<>(StringUtils.getTrimmedStringCollection(users));
      if (existUsers == null) {
        ipUsers.put(blacklistIP, newUsers);
      } else {
        existUsers.addAll(newUsers);
      }
    }

    private MD5Hash getMD5Hash(String blackListFile) {
      try {
        return MD5FileUtils.computeMd5ForFile(new File(blackListFile));
      } catch (IOException e) {
        LOG.error("Failed to get checksum for {}", blacklistFile, e);
      }
      return null;
    }
  }

  // Parse IP address pattern into Object arrays.
  private Object[] parseIpPattern(String ip) {
    String[] parts = ip.split("\\.");
    Object[] ipMask = new Object[4];
    for (int i = 0; i < parts.length; i++) {
      if (!parts[i].equals("*")) {
        int value = Integer.parseInt(parts[i]);
        if (value < 0 || value > 255) {
          throw new IllegalArgumentException("Invalid IP address : " + ip);
        }
        ipMask[i] = (byte) value;
      } else {
        ipMask[i] = null;
      }
    }
    return ipMask;
  }

  /**
   * Checks whether a given user from a specific IP address is blacklist.
   *
   * @param address The IP address from connecting.
   * @param userName     The userName from connecting.
   * @throws AuthenticationException
   */
  public void checkBlacklist(InetAddress address, String userName)
      throws AuthenticationException {
    long start = Time.now();
    try {
      if (address == null) {
        return;
      }
      ConcurrentHashMap<BlacklistIP, Set<String>> ipUsers = ipUsersCache.get();
      if (ipUsers == null || ipUsers.isEmpty()) {
        return;
      }
      if (isInBlacklist(ipUsers, address, userName)) {
        String msg = String.format("User: %s from %s in black list not allow hadoop service.",
            userName, address.getHostAddress());
        throw new AuthenticationException(msg);
      }
    } finally {
      metrics.checkBlacklist.add(Time.now() - start);
    }
  }

  private boolean isInBlacklist(ConcurrentHashMap<BlacklistIP, Set<String>> ipUsers,
      InetAddress address, String userName) {
    try {
      byte[] ipBytes = address.getAddress();
      // Check if the IP matches any pattern in the map.
      for (BlacklistIP blacklistIP : ipUsers.keySet()) {
        if (matchIp(blacklistIP.getMask(), ipBytes)) {
          Set<String> userNames = ipUsers.get(blacklistIP);
          if (userNames.contains(userName) || userNames.contains("*")) {
            return true;
          }
        }
      }
    } catch (Exception e) {
      LOG.warn("Invalid IP address: {} and userName: {}", address.getHostAddress(), userName);
    }
    // Not in the blacklist.
    return false;
  }

  private boolean matchIp(Object[] mask, byte[] ip) {
    for (int j = 0; j < 4; j++) {
      if (mask[j] != null && (byte) mask[j] != ip[j]) {
        return false;
      }
    }
    return true;
  }

  private static class BlacklistIP {
    private final String ip;
    private final Object[] mask;

    protected BlacklistIP(String ip, Object[] mask) {
      this.ip = ip;
      this.mask = mask;
    }

    public Object[] getMask() {
      return mask;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      BlacklistIP that = (BlacklistIP) o;
      return Objects.equals(ip, that.ip);
    }

    @Override
    public int hashCode() {
      return Objects.hash(ip);
    }
  }

  @VisibleForTesting
  protected static IPUsersBlacklist getInstanceForTesting(Configuration conf) {
    return new IPUsersBlacklist(conf);
  }

  @VisibleForTesting
  protected void setRefreshInterval(long refreshInterval) {
    this.refreshInterval = refreshInterval;
    if (refreshTask != null) {
      refreshTask.cancel(true);
    }
    refreshTask = scheduledExecutor.scheduleWithFixedDelay(
        ipUsersRefreshService, refreshInterval, refreshInterval, TimeUnit.MILLISECONDS);
  }

  @VisibleForTesting
  public long getRefreshInterval() {
    return refreshInterval;
  }

  @VisibleForTesting
  protected AtomicReference<ConcurrentHashMap<BlacklistIP, Set<String>>> getIpUsersCache() {
    return ipUsersCache;
  }

  @VisibleForTesting
  protected IPUsersRefreshService getIpUsersRefreshService() {
    return ipUsersRefreshService;
  }

  @VisibleForTesting
  protected void setBlacklistFile(String blacklistFile) {
    this.blacklistFile = blacklistFile;
  }

  @VisibleForTesting
  protected void resetMetrics() {
    metrics = IPUsersBlacklist.BlacklistMetrics.create();
  }
}