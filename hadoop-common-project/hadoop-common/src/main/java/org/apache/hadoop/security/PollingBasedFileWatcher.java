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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A generic file watcher that tracks a local file and calls {@link PollingBasedFileWatcher#onModified}
 * when it detects changes during each poll.
 * <br>
 * Changes are detected by using date modified.
 */
public abstract class PollingBasedFileWatcher extends Configured implements Closeable {
  public static final String NAMENODE_SIGNAL = "org.apache.hadoop.hdfs.server.namenode.NameNode";

  public static final Logger LOG = LoggerFactory.getLogger(PollingBasedFileWatcher.class);
  protected File file;
  protected String trackFile;
  protected long pollingMs;
  protected long forceMs;
  protected boolean isForceRefreshEnabled;
  protected volatile boolean initialized = false;
  private final AtomicLong lastRefreshed = new AtomicLong(-1);
  private final AtomicLong lastForceRefreshed = new AtomicLong(-1);

  private ScheduledExecutorService scheduledExecutor =
      HadoopExecutors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setDaemon(true)
          .setNameFormat("PollingBasedFileWatcher-" + this.getClass().getName()).build());
  private ScheduledFuture<?> task;

  protected void updateParams(String trackFile, long pollingMs, long forceMs) {
    String serverCategory = getConf().get(CommonConfigurationKeys.IPC_SERVER_RPC_CATEGORY_INTERNAL,
        CommonConfigurationKeys.IPC_SERVER_RPC_CATEGORY_INTERNAL_DEFAULT);
    boolean isNamenode = serverCategory.equals(NAMENODE_SIGNAL);
    this.trackFile = trackFile;
    this.pollingMs = pollingMs;
    this.forceMs = forceMs;
    this.isForceRefreshEnabled = this.forceMs > 0;

    file = new File(trackFile);
    /*
    Throw exception and prevent namenode from starting if any of these checks fail
    Other non-namenode services that make use of PollingBasedFileWatcher (e.g. OzoneManager) can set
    configurations accordingly to utilize the class.
     */
    if (!file.exists()) {
      if (isNamenode) {
        throw new RuntimeException(String.format("Tracked file %s does not exist.", trackFile));
      } else {
        return;
      }
    }
    if (!file.isFile()) {
      if (isNamenode) {
        throw new RuntimeException(String.format("Tracked file %s is not a file.", trackFile));
      } else {
        return;
      }
    }
    if (pollingMs <= 0) {
      if (isNamenode) {
        throw new RuntimeException(String.format("Invalid refresh interval %d.", pollingMs));
      } else {
        return;
      }
    }
    initialize();
  }

  protected void initialize() {
    if (initialized) {
      return;
    }
    LOG.info(
        "Starting FileWatcher for {} with params file={} pollingMs={} forceMs={}(enabled={})",
        this.getClass().getName(), trackFile, pollingMs, forceMs, isForceRefreshEnabled);
    initialized = true;
    triggerOnModified();
    task = scheduledExecutor.scheduleWithFixedDelay(new WatcherService(), pollingMs, pollingMs,
        TimeUnit.MILLISECONDS);
  }

  @Override
  public void close() throws IOException {
    if (task != null) {
      task.cancel(true);
    }
    if (scheduledExecutor != null) {
      scheduledExecutor.shutdown();
    }
  }

  public abstract boolean onModified();

  private void triggerOnModified() {
    long now = Time.monotonicNow();
    long forceDelta = now - lastForceRefreshed.get();
    long lastModified = file.lastModified();
    long lastRefreshedSnapshot = lastRefreshed.get();
    long delta = lastModified - lastRefreshedSnapshot;
    if (isForceRefreshEnabled && forceDelta > forceMs) {
      LOG.info("Last force refresh was {}ms ago, initiating another", forceDelta);
      onModified();
      lastRefreshed.set(lastModified);
      lastForceRefreshed.set(now);
    } else if (delta != 0) {
      LOG.info("Change detected in {}", trackFile);
      onModified();
      lastRefreshed.set(lastModified);
    }
  }

  class WatcherService implements Runnable {
    @Override
    public void run() {
      triggerOnModified();
    }
  }


  @VisibleForTesting
  public long getLastRefreshed() {
    return lastRefreshed.get();
  }
}
