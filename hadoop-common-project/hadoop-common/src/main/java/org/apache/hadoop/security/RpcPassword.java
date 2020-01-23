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
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.*;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Timer;
import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A user-to-rpcPassword mapping service.
 * 
 * {@link RpcPassword} allows for server to get the rpcPassword of a given user
 * via the {@link #getRpcPassword(String)} call
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class RpcPassword {
  private static final Logger LOG = LoggerFactory.getLogger(RpcPassword.class);

  private final RpcPasswordMappingServiceProvider impl;

  private final LoadingCache<String, RpcPasswordAndBypass> cache;
  private final long cacheTimeout;
  private final long negativeCacheTimeout;
  private final long warningDeltaMs;
  private final Timer timer;
  private Set<String> negativeCache;
  private final boolean reloadRpcPasswordInBackground;
  private final int reloadRpcPasswordThreadCount;

  private final AtomicLong backgroundRefreshSuccess =
      new AtomicLong(0);
  private final AtomicLong backgroundRefreshException =
      new AtomicLong(0);
  private final AtomicLong backgroundRefreshQueued =
      new AtomicLong(0);
  private final AtomicLong backgroundRefreshRunning =
      new AtomicLong(0);

  public RpcPassword(Configuration conf) {
    this(conf, new Timer());
  }

  public RpcPassword(Configuration conf, final Timer timer) {
    impl = 
      ReflectionUtils.newInstance(
          conf.getClass(CommonConfigurationKeys.HADOOP_SECURITY_RPC_PASSWORD_MAPPING,
                        ShadowFileRpcPasswordMapping.class,
                        RpcPasswordMappingServiceProvider.class),
          conf);

    cacheTimeout =
      conf.getLong(CommonConfigurationKeys.HADOOP_SECURITY_RPC_PASSWORD_CACHE_SECS,
          CommonConfigurationKeys.HADOOP_SECURITY_RPC_PASSWORD_CACHE_SECS_DEFAULT) * 1000;
    negativeCacheTimeout =
            conf.getLong(CommonConfigurationKeys.HADOOP_SECURITY_RPC_PASSWORD_NEGATIVE_CACHE_SECS,
                    CommonConfigurationKeys.HADOOP_SECURITY_RPC_PASSWORD_NEGATIVE_CACHE_SECS_DEFAULT) * 1000;
    warningDeltaMs =
      conf.getLong(CommonConfigurationKeys.HADOOP_SECURITY_RPC_PASSWORD_CACHE_WARN_AFTER_MS,
        CommonConfigurationKeys.HADOOP_SECURITY_RPC_PASSWORD_CACHE_WARN_AFTER_MS_DEFAULT);
    reloadRpcPasswordInBackground =
      conf.getBoolean(
          CommonConfigurationKeys.
              HADOOP_SECURITY_RPC_PASSWORD_CACHE_BACKGROUND_RELOAD,
          CommonConfigurationKeys.
              HADOOP_SECURITY_RPC_PASSWORD_CACHE_BACKGROUND_RELOAD_DEFAULT);
    reloadRpcPasswordThreadCount  =
      conf.getInt(
          CommonConfigurationKeys.
              HADOOP_SECURITY_RPC_PASSWORD_CACHE_BACKGROUND_RELOAD_THREADS,
          CommonConfigurationKeys.
              HADOOP_SECURITY_RPC_PASSWORD_CACHE_BACKGROUND_RELOAD_THREADS_DEFAULT);

    this.timer = timer;
    this.cache = CacheBuilder.newBuilder()
      .refreshAfterWrite(cacheTimeout, TimeUnit.MILLISECONDS)
      .ticker(new TimerToTickerAdapter(timer))
      .expireAfterWrite(10 * cacheTimeout, TimeUnit.MILLISECONDS)
      .build(new RpcPasswordCacheLoader());

    if (negativeCacheTimeout > 0) {
      Cache<String, Boolean> tempMap = CacheBuilder.newBuilder()
              .expireAfterWrite(negativeCacheTimeout, TimeUnit.MILLISECONDS)
              .ticker(new TimerToTickerAdapter(timer))
              .build();
      negativeCache = Collections.newSetFromMap(tempMap.asMap());
    }

    if (LOG.isDebugEnabled())
      LOG.debug("Rpc Password mapping impl=" + impl.getClass().getName() +
          "; cacheTimeout=" + cacheTimeout + "; warningDeltaMs=" +
          warningDeltaMs);
  }
  
  @VisibleForTesting
  Set<String> getNegativeCache() {
    return negativeCache;
  }

  private boolean isNegativeCacheEnabled() {
    return negativeCacheTimeout > 0;
  }

  private IOException noRpcPasswordForUser(String user) {
    return new IOException("No rpc password found for user " + user);
  }

  /**
   * Get the group memberships of a given user.
   * If the user's group is not cached, this method may block.
   * @param user User's name
   * @return the group memberships of the user
   * @throws IOException if user does not exist
   */
  public String getRpcPassword(final String user) throws IOException {
    // Check the negative cache first
    if (isNegativeCacheEnabled()) {
      if (negativeCache.contains(user)) {
        throw noRpcPasswordForUser(user);
      }
    }

    try {
      return cache.get(user).getRpcPassword();
    } catch (ExecutionException e) {
      throw (IOException)e.getCause();
    }
  }

  /**
   * Check if the given user is a bypass user
   * @param user User's name
   * @return if it's a bypass user
   * @throws IOException if user does not exist
   */
  public boolean isBypassUser(final String user) throws IOException {
    // Check the negative cache first
    if (isNegativeCacheEnabled()) {
      if (negativeCache.contains(user)) {
        throw noRpcPasswordForUser(user);
      }
    }

    try {
      return cache.get(user).isBypass();
    } catch (ExecutionException e) {
      throw (IOException)e.getCause();
    }
  }

  public long getBackgroundRefreshSuccess() {
    return backgroundRefreshSuccess.get();
  }

  public long getBackgroundRefreshException() {
    return backgroundRefreshException.get();
  }

  public long getBackgroundRefreshQueued() {
    return backgroundRefreshQueued.get();
  }

  public long getBackgroundRefreshRunning() {
    return backgroundRefreshRunning.get();
  }

  /**
   * Convert millisecond times from hadoop's timer to guava's nanosecond ticker.
   */
  private static class TimerToTickerAdapter extends Ticker {
    private Timer timer;

    public TimerToTickerAdapter(Timer timer) {
      this.timer = timer;
    }

    @Override
    public long read() {
      final long NANOSECONDS_PER_MS = 1000000;
      return timer.monotonicNow() * NANOSECONDS_PER_MS;
    }
  }

  /**
   * Deals with loading data into the cache.
   */
  private class RpcPasswordCacheLoader extends CacheLoader<String, RpcPasswordAndBypass> {

    private ListeningExecutorService executorService;

    RpcPasswordCacheLoader() {
      if (reloadRpcPasswordInBackground) {
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
            .setNameFormat("RpcPassword-Cache-Reload")
            .setDaemon(true)
            .build();
        // With coreThreadCount == maxThreadCount we effectively
        // create a fixed size thread pool. As allowCoreThreadTimeOut
        // has been set, all threads will die after 60 seconds of non use
        ThreadPoolExecutor parentExecutor =  new ThreadPoolExecutor(
            reloadRpcPasswordThreadCount,
            reloadRpcPasswordThreadCount,
            60,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(),
            threadFactory);
        parentExecutor.allowCoreThreadTimeOut(true);
        executorService = MoreExecutors.listeningDecorator(parentExecutor);
      }
    }

    /**
     * This method will block if a cache entry doesn't exist, and
     * any subsequent requests for the same user will wait on this
     * request to return. If a user already exists in the cache,
     * and when the key expires, the first call to reload the key
     * will block, but subsequent requests will return the old
     * value until the blocking thread returns.
     * If reloadGroupsInBackground is true, then the thread that
     * needs to refresh an expired key will not block either. Instead
     * it will return the old cache value and schedule a background
     * refresh
     * @param user key of cache
     * @return List of groups belonging to user
     * @throws IOException to prevent caching negative entries
     */
    @Override
    public RpcPasswordAndBypass load(String user) throws Exception {
      TraceScope scope = null;
      Tracer tracer = Tracer.curThreadTracer();
      if (tracer != null) {
        scope = tracer.newScope("RpcPassword#fetchRpcPassword");
        scope.addKVAnnotation("user", user);
      }
      RpcPasswordAndBypass rpcPasswordAndBypass = null;
      try {
        rpcPasswordAndBypass = fetchRpcPasswordAndBypass(user);
      } finally {
        if (scope != null) {
          scope.close();
        }
      }

      if (rpcPasswordAndBypass.isEmpty()) {
        if (isNegativeCacheEnabled()) {
          negativeCache.add(user);
        }

        // We throw here to prevent Cache from retaining an empty password
        throw noRpcPasswordForUser(user);
      }

      // return RpcPasswordAndBypass
      return rpcPasswordAndBypass;
    }

    /**
     * Override the reload method to provide an asynchronous implementation. If
     * reloadGroupsInBackground is false, then this method defers to the super
     * implementation, otherwise is arranges for the cache to be updated later
     */
    @Override
    public ListenableFuture<RpcPasswordAndBypass> reload(final String key,
                                                 RpcPasswordAndBypass oldValue)
        throws Exception {
      if (!reloadRpcPasswordInBackground) {
        return super.reload(key, oldValue);
      }

      backgroundRefreshQueued.incrementAndGet();
      ListenableFuture<RpcPasswordAndBypass> listenableFuture =
          executorService.submit(new Callable<RpcPasswordAndBypass>() {
            @Override
            public RpcPasswordAndBypass call() throws Exception {
              backgroundRefreshQueued.decrementAndGet();
              backgroundRefreshRunning.incrementAndGet();
              RpcPasswordAndBypass result = load(key);
              return result;
            }
          });
      Futures.addCallback(listenableFuture, new FutureCallback<RpcPasswordAndBypass>() {
        @Override
        public void onSuccess(RpcPasswordAndBypass result) {
          backgroundRefreshSuccess.incrementAndGet();
          backgroundRefreshRunning.decrementAndGet();
        }
        @Override
        public void onFailure(Throwable t) {
          backgroundRefreshException.incrementAndGet();
          backgroundRefreshRunning.decrementAndGet();
        }
      });
      return listenableFuture;
    }

    /**
     * Queries impl for rpc password belonging to the user. This could involve I/O and take awhile.
     */
    private RpcPasswordAndBypass fetchRpcPasswordAndBypass(String user) throws IOException {
      long startMs = timer.monotonicNow();
      String rpcPassword = impl.getRpcPassword(user);
      boolean isBypass = impl.isBypassUser(user);
      long endMs = timer.monotonicNow();
      long deltaMs = endMs - startMs ;
      if (deltaMs > warningDeltaMs) {
        LOG.warn("Potential performance problem: getRpcPassword(user=" + user +") " +
          "took " + deltaMs + " milliseconds.");
      }

      return new RpcPasswordAndBypass(rpcPassword, isBypass);
    }
  }

  /**
   * Refresh all user-to-rpcPassword mappings.
   */
  public void refresh() throws IOException{
    LOG.info("clearing userToRpcPasswordMap cache");
    cache.invalidateAll();
    if (isNegativeCacheEnabled()) {
      negativeCache.clear();
    }
    impl.cacheRefresh(true);
  }

  private static RpcPassword RPCPASSWORD= null;
  
  /**
   * Get the RpcPassword being used to map user-to-rpcPassword.
   * @return the rpcPassword being used to map user-to-rpcPassword.
   */
  public static RpcPassword getUserToRpcPasswordMappingService() {
    return getUserToRpcPasswordMappingService(new Configuration());
  }

  /**
   * Get the RpcPassword being used to map user-to-rpcPassword.
   * @param conf
   * @return the rpcPassword being used to map user-to-rpcPassword.
   */
  public static synchronized RpcPassword getUserToRpcPasswordMappingService(
    Configuration conf) {

    if (RPCPASSWORD == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(" Creating new RpcPassword object");
      }
      RPCPASSWORD = new RpcPassword(conf);
    }
    return RPCPASSWORD;
  }

  /**
   * Create new RpcPassword used to map user-to-rpcPassword with loaded configuration.
   * @param conf
   * @return the RpcPassword being used to map user-to-rpcPassword.
   */
  @Private
  public static synchronized RpcPassword
      getUserToRpcPasswordMappingServiceWithLoadedConfiguration(
          Configuration conf) {

    RPCPASSWORD = new RpcPassword(conf);
    return RPCPASSWORD;
  }


}
