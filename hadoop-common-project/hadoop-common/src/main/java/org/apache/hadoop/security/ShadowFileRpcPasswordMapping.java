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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE_CACHE_REFRESH_INTERVAL_DEFAULT;

/**
 * A simple shadow file based implementation of
 * {@link RpcPasswordMappingServiceProvider}.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class ShadowFileRpcPasswordMapping extends Configured
    implements RpcPasswordMappingServiceProvider {

  @VisibleForTesting
  protected static final Logger LOG =
      LoggerFactory.getLogger(ShadowFileRpcPasswordMapping.class);

  private String shadowFile = CommonConfigurationKeys.
      HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE_DEFAULT;
  private static final String EMPTY_PASSWORD = null;

  private long cacheTimeout = CommonConfigurationKeys.
      HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE_CACHE_SEC_DEFAULT * 1000;
  private long cacheRefreshInterval =
      HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE_CACHE_REFRESH_INTERVAL_DEFAULT * 1000;
  private boolean cacheRefreshAsync;
  private volatile long lastRefreshTime = -1L;
  private static final ScheduledExecutorService scheduledExecutor =
      HadoopExecutors.newSingleThreadScheduledExecutor(
          new ThreadFactoryBuilder().setDaemon(true)
              .setNameFormat("LocalRPCPasswordCacheRefresh").build());
  private ScheduledFuture<?> refreshTask;
  private CacheRefreshService cacheRefreshService;

  private ConcurrentHashMap<String, RpcPasswordAndBypass> cache =
      new ConcurrentHashMap<>();

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (conf != null) {
      shadowFile = conf.get(
          CommonConfigurationKeys.
              HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE,
          CommonConfigurationKeys.
              HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE_DEFAULT);
      cacheTimeout = conf.getLong(
          CommonConfigurationKeys.HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE_CACHE_SEC,
          CommonConfigurationKeys.HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE_CACHE_SEC_DEFAULT) * 1000;

      cacheRefreshInterval = conf.getLong(
          CommonConfigurationKeys.
              HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE_CACHE_REFRESH_INTERVAL,
          CommonConfigurationKeys.
              HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE_CACHE_REFRESH_INTERVAL_DEFAULT)
          * 1000;

      cacheRefreshAsync = conf.getBoolean(
          CommonConfigurationKeys.
              HADOOP_SECURITY_RPC_PASSWORD_CACHE_REFRESH_ASYNC,
          CommonConfigurationKeys.
              HADOOP_SECURITY_RPC_PASSWORD_CACHE_REFRESH_ASYNC_DEFAULT);
    }
  }

  @Override
  public void start() {
    if (cacheRefreshService == null && cacheRefreshAsync) {
      LOG.info("Initialize PRCpassword cache refresh async service!");
      initializeCacheRefreshService();
    }
  }

  protected void initializeCacheRefreshService() {
    if (cacheRefreshInterval <= 0) {
      LOG.warn("Invalid refresh interval: {}", cacheRefreshInterval);
      return;
    }
    cacheRefreshService = new CacheRefreshService();
    try {
      cacheRefresh(true);
    } catch (IOException e) {
      LOG.error("Failed to run cache refresh", e);
    }
    refreshTask = scheduledExecutor
        .scheduleWithFixedDelay(cacheRefreshService, cacheRefreshInterval, cacheRefreshInterval,
            TimeUnit.MILLISECONDS);
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
      if ((isTimeout() || cache.size() == 0) && !cacheRefreshAsync) {
        cacheRefresh(false);
      }
      if (!cache.containsKey(userName)){
        return null;
      }
      return cache.get(userName).getRpcPassword();
    } catch (IOException e){
      LOG.error("Failed to refresh cache!", e);
    }
    return null;
  }

  @Override
  public boolean isBypassUser(String user) {
    try {
      if ((isTimeout() || cache.size() == 0)  && !cacheRefreshAsync) {
        cacheRefresh(false);
      }
      if (!cache.containsKey(user)){
        return false;
      }
      return cache.get(user).isBypass();
    } catch (IOException e){
      LOG.error("Failed to refresh cache!", e);
    }
    return false;
  }

  class CacheRefreshService implements Runnable {
    @Override
    public void run() {
      try {
        cacheRefresh(true);
      } catch (IOException e) {
        LOG.error("Failed to refresh cache!", e);
      }
    }
  }

  @Override
  public void cacheRefresh(boolean force) throws IOException {
    if (!force){
      // If not force refresh, check the timeout again
      if (!isTimeout())
        return;
    }
    BufferedReader br = null;
    try {
      cache.clear();
      FileInputStream file = new FileInputStream(shadowFile);
      Reader fr = new InputStreamReader(file, StandardCharsets.UTF_8);
      br = new BufferedReader(fr);
      String line;
      while ((line = br.readLine()) != null) {
        //process the line
        try {
          processRow(line);
        } catch (IllegalShadowLineException e) {
          LOG.warn("unable to process shadow line: {}", line, e);
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Refreshed " + cache.size() + "records from shadowFile.");
      }
      lastRefreshTime = Time.now();
    }catch (IOException e){
      e.printStackTrace();
    }finally {
      if (br != null){
        br.close();
      }
    }
  }

  private void processRow(String string) throws IllegalShadowLineException {
    // handle comment line
    if(string.startsWith("#"))
      return;
    if(string.split(",").length != 3){
      throw new IllegalShadowLineException(string);
    }
    String user = string.split(",")[0];
    String shadow = string.split(",")[1];
    boolean bypass = string.split(",")[2].equalsIgnoreCase("true");
    cache.put(user, new RpcPasswordAndBypass(shadow, bypass));
  }

  private boolean isTimeout(){
    return Time.now() - lastRefreshTime > cacheTimeout;
  }

  private static class IllegalShadowLineException extends IOException {
    public IllegalShadowLineException(String message) {
      super(message);
    }

    public IllegalShadowLineException(String message, Throwable err) {
      super(message, err);
    }

    @Override
    public String toString() {
      final StringBuilder sb =
          new StringBuilder("IllegalShadowLineException ");
      sb.append(super.getMessage());
      return sb.toString();
    }
  }
}
