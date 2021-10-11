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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.hash.MD5FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.Reader;
import java.lang.instrument.ClassFileTransformer;
import java.nio.charset.StandardCharsets;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A simple shadow file based implementation of {@link RpcPasswordMappingServiceProvider}
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
  private boolean checksumEnabled = CommonConfigurationKeys.
      HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE_CHECKSUM_ENABLED_DEFAULT;

  private long cacheTimeout =
      CommonConfigurationKeys.
          HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE_CACHE_SEC_DEFAULT * 1000;
  private AtomicLong lastRefreshTime = new AtomicLong(-1L);
  private volatile boolean isStartup = true;

  private AtomicReference<ConcurrentHashMap<String, RpcPasswordAndBypass>>
      cacheRef = new AtomicReference<>();

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (conf != null) {
      shadowFile = conf.get(
          CommonConfigurationKeys.
              HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE,
          CommonConfigurationKeys.
              HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE_DEFAULT);
      checksumEnabled = conf.getBoolean(
          CommonConfigurationKeys.
              HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE_CHECKSUM_ENABLED,
          CommonConfigurationKeys.
              HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE_CHECKSUM_ENABLED_DEFAULT
      );
      cacheTimeout = conf.getLong(
          CommonConfigurationKeys.
              HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE_CACHE_SEC,
          CommonConfigurationKeys.
              HADOOP_SECURITY_RPC_PASSWORD_SHADOW_FILE_CACHE_SEC_DEFAULT)
          * 1000;
    }
  }

  /**
   * Returns rpcPassword for a user
   *
   * @param userName get rpcPassword for this user
   * @return list of rpcPassword for a given user
   */
  @Override
  public String getRpcPassword(String userName) {
    try {
      if (cacheRef.get() == null || cacheRef.get().size() == 0) {
        cacheRefresh(true);
      }
      if (isTimeout()) {
        cacheRefresh(false);
      }
      if (!cacheRef.get().containsKey(userName)) {
        return null;
      }
      return cacheRef.get().get(userName).getRpcPassword();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public boolean isBypassUser(String user) {
    try {
      if (cacheRef.get() == null || cacheRef.get().size() == 0) {
        cacheRefresh(true);
      }
      if (isTimeout()) {
        cacheRefresh(false);
      }
      if (!cacheRef.get().containsKey(user)) {
        return false;
      }
      return cacheRef.get().get(user).isBypass();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return false;
  }

  @Override
  public void cacheRefresh(boolean force) throws IOException {
    if (!force) {
      // If not force refresh, check the timeout again
      if (!isTimeout())
        return;
    }
    BufferedReader br = null;
    ConcurrentHashMap<String, RpcPasswordAndBypass> updateCache =
        new ConcurrentHashMap<>();

    MD5Hash md5Hash = null;
    if (checksumEnabled && !isStartup) {
      md5Hash = checksum();
      if (md5Hash == null) {
        LOG.error("First round checksum not match. Password not refreshed.");
        lastRefreshTime.set(Time.now());
        return;
      }
    }

    try {
      FileInputStream file = new FileInputStream(shadowFile);
      Reader fr = new InputStreamReader(file, StandardCharsets.UTF_8);
      br = new BufferedReader(fr);
      String line;
      while ((line = br.readLine()) != null) {
        //process the line
        try {
          processRow(updateCache, line);
        } catch (IllegalShadowLineException e) {
          LOG.warn("unable to process shadow line: {}", line, e);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (br != null) {
        br.close();
      }
    }

    if (checksumEnabled && !isStartup) {
      MD5Hash fileHash = MD5FileUtils.computeMd5ForFile(new File(shadowFile));
      if (md5Hash != null && !md5Hash.equals(fileHash)) {
        LOG.error("Second Checksum not match.");
        lastRefreshTime.set(Time.now());
        return;
      }
    }
    cacheRef.set(updateCache);
    lastRefreshTime.set(Time.now());
    LOG.info("Refreshed " + updateCache.size() + " records from shadowFile.");
    if (isStartup) {
      isStartup = false;
    }
  }

  private MD5Hash checksum() {
    MD5Hash fileHash, storedHash;
    MD5Hash result = null;
    try {
      fileHash = MD5FileUtils.computeMd5ForFile(new File(shadowFile));
      storedHash = MD5FileUtils.readStoredMd5ForFile(
          new File(shadowFile));
      if (storedHash == null) {
        LOG.error("MD5 File not exists: " +
            MD5FileUtils.getDigestFileForFile(new File(shadowFile)));
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

  private void processRow(ConcurrentHashMap<String, RpcPasswordAndBypass> cache,
      String string) throws IllegalShadowLineException {
    // handle comment line
    if (string.startsWith("#"))
      return;
    if (string.split(",").length != 3) {
      throw new IllegalShadowLineException(string);
    }
    String user = string.split(",")[0];
    String shadow = string.split(",")[1];
    boolean bypass = string.split(",")[2].equalsIgnoreCase("true");
    cache.put(user, new RpcPasswordAndBypass(shadow, bypass));
  }

  private boolean isTimeout() {
    return Time.now() - lastRefreshTime.get() > cacheTimeout;
  }

  @VisibleForTesting
  public long getLastRefreshTime() {
    return lastRefreshTime.get();
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
