package org.apache.hadoop.yarn.server.router.utils;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.utils.CacheUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import javax.cache.spi.CachingProvider;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class RouterRpcRequestCache {

  private static final Logger LOG =
      LoggerFactory.getLogger(RouterRpcRequestCache.class);

  private static final RouterRpcRequestCache requestsCache =
      new RouterRpcRequestCache();

  private Configuration conf;
  private Cache<Object, Object> cache;
  private int cacheTimeToLive;

  private RouterRpcRequestCache(){
    this.conf = new Configuration();
    initCache();
  }

  private void initCache() {
    // Picking the JCache provider from classpath, need to make sure there's
    // no conflict or pick up a specific one in the future
    cacheTimeToLive =
        this.conf.getInt(YarnConfiguration.ROUTER_RPC_CACHE_TIME_TO_LIVE_SECS,
            YarnConfiguration.DEFAULT_ROUTER_RPC_CACHE_TIME_TO_LIVE_SECS);
    LOG.info("RouterRpcRequestCache cacheTimeToLive = " + cacheTimeToLive);

    if (isCachingEnabled()) {
      CachingProvider jcacheProvider = Caching.getCachingProvider();
      CacheManager jcacheManager = jcacheProvider.getCacheManager();
      this.cache = jcacheManager.getCache(this.getClass().getSimpleName());
      if (this.cache == null) {
        LOG.info("Creating a JCache Manager with name "
            + this.getClass().getSimpleName());
        Duration cacheExpiry = new Duration(TimeUnit.SECONDS, cacheTimeToLive);
        CompleteConfiguration<Object, Object> configuration =
            new MutableConfiguration<Object, Object>().setStoreByValue(false)
                .setReadThrough(true)
                .setExpiryPolicyFactory(
                    new FactoryBuilder.SingletonFactory<ExpiryPolicy>(
                        new CreatedExpiryPolicy(cacheExpiry)))
                .setCacheLoaderFactory(
                    new FactoryBuilder.SingletonFactory<CacheLoader<Object, Object>>(
                        new CacheUtil.CacheLoaderImpl<Object, Object>()));
        this.cache = jcacheManager.createCache(this.getClass().getSimpleName(),
            configuration);
      }
    }
  }

  public boolean isCachingEnabled() {
    return (cacheTimeToLive > 0);
  }

  /**
   * Delete and re-initialize the cache, to force it to use the given
   * configuration.
   *
   * @param config the updated configuration to reinitialize with
   */
  @VisibleForTesting
  public synchronized void reinitialize(Configuration config) {
    this.conf = config;
    initCache();
  }

  /**
   * Returns the singleton instance of the RouterRpcRequestCache object.
   *
   * @return the singleton {@link RouterRpcRequestCache} instance
   */
  public static RouterRpcRequestCache getInstance() {
    return requestsCache;
  }

  public Cache<Object, Object> getCache(){
    return this.cache;
  }

}
