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

package org.apache.hadoop.yarn.server.federation.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.cache.integration.CacheLoaderException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.federation.cache.FederationCache;
import org.apache.hadoop.yarn.server.federation.resolver.SubClusterResolver;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.exception.FederationStateStoreRetriableException;
import org.apache.hadoop.yarn.server.federation.store.records.AddApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.AddApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteSubClusterPolicyConfigurationRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationsHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationsHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterInfoRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterInfoResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPoliciesConfigurationsRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPolicyConfigurationRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPolicyConfigurationResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClustersInfoRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SetSubClusterPolicyConfigurationRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterDeregisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateApplicationHomeSubClusterRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import com.zaxxer.hikari.pool.HikariPool.PoolInitializationException;

import static org.apache.hadoop.yarn.server.federation.cache.FederationCache.buildPolicyConfigMap;
import static org.apache.hadoop.yarn.server.federation.cache.FederationCache.buildSubClusterInfoMap;

/**
 *
 * The FederationStateStoreFacade is an utility wrapper that provides singleton
 * access to the Federation state store. It abstracts out retries and in
 * addition, it also implements the caching for various objects.
 *
 */
public final class FederationStateStoreFacade {
  private static final Logger LOG =
      LoggerFactory.getLogger(FederationStateStoreFacade.class);

  private static final FederationStateStoreFacade FACADE =
      new FederationStateStoreFacade();

  private FederationStateStore stateStore;
  private Configuration conf;
  private SubClusterResolver subclusterResolver;
  private FederationCache federationCache;

  private FederationStateStoreFacade() {
    initializeFacadeInternal(new Configuration());
  }

  private void initializeFacadeInternal(Configuration config) {
    this.conf = config;
    try {
      this.stateStore = (FederationStateStore) createRetryInstance(this.conf,
          YarnConfiguration.FEDERATION_STATESTORE_CLIENT_CLASS,
          YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_CLIENT_CLASS,
          FederationStateStore.class, createRetryPolicy(conf));
      this.stateStore.init(conf);

      this.subclusterResolver = createInstance(conf,
          YarnConfiguration.FEDERATION_CLUSTER_RESOLVER_CLASS,
          YarnConfiguration.DEFAULT_FEDERATION_CLUSTER_RESOLVER_CLASS,
          SubClusterResolver.class);
      this.subclusterResolver.load();

      // We check the configuration of Cache,
      // if the configuration is null, set it to FederationJCache
      this.federationCache = createInstance(conf,
          YarnConfiguration.FEDERATION_FACADE_CACHE_CLASS,
          YarnConfiguration.DEFAULT_FEDERATION_FACADE_CACHE_CLASS,
          FederationCache.class);
      this.federationCache.initCache(config, stateStore);

    } catch (YarnException ex) {
      LOG.error("Failed to initialize the FederationStateStoreFacade object",
          ex);
      throw new RuntimeException(ex);
    }
  }

  /**
   * Delete and re-initialize the cache, to force it to use the given
   * configuration.
   *
   * @param store the {@link FederationStateStore} instance to reinitialize with
   * @param config the updated configuration to reinitialize with
   */
  @VisibleForTesting
  public synchronized void reinitialize(FederationStateStore store,
      Configuration config) {
    this.conf = config;
    this.stateStore = store;
    federationCache.clearCache();
    federationCache.initCache(config, stateStore);
  }

  /**
   * Create a RetryPolicy for {@code FederationStateStoreFacade}. In case of
   * failure, it retries for:
   * <ul>
   * <li>{@code FederationStateStoreRetriableException}</li>
   * <li>{@code CacheLoaderException}</li>
   * </ul>
   *
   * @param conf the updated configuration
   * @return the RetryPolicy for FederationStateStoreFacade
   */
  public static RetryPolicy createRetryPolicy(Configuration conf) {
    // Retry settings for StateStore
    RetryPolicy basePolicy = RetryPolicies.exponentialBackoffRetry(
        conf.getInt(YarnConfiguration.CLIENT_FAILOVER_RETRIES, Integer.SIZE),
        conf.getLong(YarnConfiguration.CLIENT_FAILOVER_SLEEPTIME_BASE_MS,
            YarnConfiguration.DEFAULT_RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS),
        TimeUnit.MILLISECONDS);
    Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap = new HashMap<>();
    exceptionToPolicyMap.put(FederationStateStoreRetriableException.class,
        basePolicy);
    exceptionToPolicyMap.put(CacheLoaderException.class, basePolicy);
    exceptionToPolicyMap.put(PoolInitializationException.class, basePolicy);

    RetryPolicy retryPolicy = RetryPolicies.retryByException(
        RetryPolicies.TRY_ONCE_THEN_FAIL, exceptionToPolicyMap);
    return retryPolicy;
  }

  /**
   * Returns the singleton instance of the FederationStateStoreFacade object.
   *
   * @return the singleton {@link FederationStateStoreFacade} instance
   */
  public static FederationStateStoreFacade getInstance() {
    return FACADE;
  }

  /**
   * Deregister a <em>subcluster</em> identified by {@code SubClusterId} to
   * change state in federation. This can be done to mark the sub cluster lost,
   * deregistered, or decommissioned.
   *
   * @param subClusterId the target subclusterId
   * @param subClusterState the state to update it to
   * @throws YarnException if the request is invalid/fails
   */
  public void deregisterSubCluster(SubClusterId subClusterId,
      SubClusterState subClusterState) throws YarnException {
    stateStore.deregisterSubCluster(
        SubClusterDeregisterRequest.newInstance(subClusterId, subClusterState));
    return;
  }

  /**
   * Returns the {@link SubClusterInfo} for the specified {@link SubClusterId}.
   *
   * @param subClusterId the identifier of the sub-cluster
   * @return the sub cluster information, or
   *         {@code null} if there is no mapping for the subClusterId
   * @throws YarnException if the call to the state store is unsuccessful
   */
  public SubClusterInfo getSubCluster(final SubClusterId subClusterId)
      throws YarnException {
    if (federationCache.isCachingEnabled()) {
      return getSubClusters(false).get(subClusterId);
    } else {
      GetSubClusterInfoResponse response = stateStore
          .getSubCluster(GetSubClusterInfoRequest.newInstance(subClusterId));
      if (response == null) {
        return null;
      } else {
        return response.getSubClusterInfo();
      }
    }
  }

  /**
   * Updates the cache with the central {@link FederationStateStore} and returns
   * the {@link SubClusterInfo} for the specified {@link SubClusterId}.
   *
   * @param subClusterId the identifier of the sub-cluster
   * @param flushCache flag to indicate if the cache should be flushed or not
   * @return the sub cluster information
   * @throws YarnException if the call to the state store is unsuccessful
   */
  public SubClusterInfo getSubCluster(final SubClusterId subClusterId,
      final boolean flushCache) throws YarnException {
    if (flushCache && federationCache.isCachingEnabled()) {
      LOG.info("Flushing subClusters from cache and rehydrating from store.");
      federationCache.removeSubCluster(false);
    }
    return getSubCluster(subClusterId);
  }

  /**
   * Returns the {@link SubClusterInfo} of all active sub cluster(s).
   *
   * @param filterInactiveSubClusters whether to filter out inactive
   *          sub-clusters
   * @return the information of all active sub cluster(s)
   * @throws YarnException if the call to the state store is unsuccessful
   */
  @SuppressWarnings("unchecked")
  public Map<SubClusterId, SubClusterInfo> getSubClusters(
      final boolean filterInactiveSubClusters) throws YarnException {
    try {
      if (federationCache.isCachingEnabled()) {
        return federationCache.getSubClusters(filterInactiveSubClusters);
      } else {
        GetSubClustersInfoRequest request =
            GetSubClustersInfoRequest.newInstance(filterInactiveSubClusters);
        return buildSubClusterInfoMap(stateStore.getSubClusters(request));
      }
    } catch (Throwable ex) {
      throw new YarnException(ex);
    }
  }

  /**
   * Updates the cache with the central {@link FederationStateStore} and returns
   * the {@link SubClusterInfo} of all active sub cluster(s).
   *
   * @param filterInactiveSubClusters whether to filter out inactive
   *          sub-clusters
   * @param flushCache flag to indicate if the cache should be flushed or not
   * @return the sub cluster information
   * @throws YarnException if the call to the state store is unsuccessful
   */
  public Map<SubClusterId, SubClusterInfo> getSubClusters(
      final boolean filterInactiveSubClusters, final boolean flushCache)
      throws YarnException {
    if (flushCache && federationCache.isCachingEnabled()) {
      LOG.info("Flushing subClusters from cache and rehydrating from store.");
      federationCache.removeSubCluster(filterInactiveSubClusters);
    }
    return getSubClusters(filterInactiveSubClusters);
  }

  /**
   * Returns the {@link SubClusterPolicyConfiguration} for the specified queue.
   *
   * @param queue the queue whose policy is required
   * @return the corresponding configured policy, or {@code null} if there is no
   *         mapping for the queue
   * @throws YarnException if the call to the state store is unsuccessful
   */
  public SubClusterPolicyConfiguration getPolicyConfiguration(
      final String queue) throws YarnException {
    if (federationCache.isCachingEnabled()) {
      return getPoliciesConfigurations().get(queue);
    } else {
      GetSubClusterPolicyConfigurationRequest request =
          GetSubClusterPolicyConfigurationRequest.newInstance(queue);
      GetSubClusterPolicyConfigurationResponse response =
          stateStore.getPolicyConfiguration(request);
      if (response == null) {
        return null;
      } else {
        return response.getPolicyConfiguration();
      }
    }
  }

  /**
   * Get the policies that is represented as
   * {@link SubClusterPolicyConfiguration} for all currently active queues in
   * the system.
   *
   * @return the policies for all currently active queues in the system
   * @throws YarnException if the call to the state store is unsuccessful
   */
  @SuppressWarnings("unchecked")
  public Map<String, SubClusterPolicyConfiguration> getPoliciesConfigurations()
      throws YarnException {
    try {
      if (federationCache.isCachingEnabled()) {
        return federationCache.getPoliciesConfigurations();
      } else {
        GetSubClusterPoliciesConfigurationsRequest request =
            GetSubClusterPoliciesConfigurationsRequest.newInstance();
        return buildPolicyConfigMap(stateStore.getPoliciesConfigurations(request));
      }
    } catch (Throwable ex) {
      throw new YarnException(ex);
    }
  }

  /**
   * Set a policy configuration into the state store.
   *
   * @param policyConf the policy configuration to set
   * @throws YarnException if the request is invalid/fails
   */
  public void setPolicyConfiguration(SubClusterPolicyConfiguration policyConf)
      throws YarnException {
    stateStore.setPolicyConfiguration(
        SetSubClusterPolicyConfigurationRequest.newInstance(policyConf));
  }

  /**
   * Delete the policy for a given queue
   * Currently response is empty if the operation is successful,
   * if not an exception reporting reason for a failure.
   *
   * @param queue the queue whose policy is required
   * @throws YarnException if the call to the state store is unsuccessful
   */
  public void deletePolicyConfiguration(final String queue)
      throws YarnException {
    stateStore.deletePolicyConfiguration(
        DeleteSubClusterPolicyConfigurationRequest.newInstance(queue));
  }

  /**
   * Adds the home {@link SubClusterId} for the specified {@link ApplicationId}.
   *
   * @param appHomeSubCluster the mapping of the application to it's home
   *          sub-cluster
   * @return the stored Subcluster from StateStore
   * @throws YarnException if the call to the state store is unsuccessful
   */
  public SubClusterId addApplicationHomeSubCluster(
      ApplicationHomeSubCluster appHomeSubCluster) throws YarnException {
    AddApplicationHomeSubClusterResponse response =
        stateStore.addApplicationHomeSubCluster(
            AddApplicationHomeSubClusterRequest.newInstance(appHomeSubCluster));
    return response.getHomeSubCluster();
  }

  /**
   * Updates the home {@link SubClusterId} for the specified
   * {@link ApplicationId}.
   *
   * @param appHomeSubCluster the mapping of the application to it's home
   *          sub-cluster
   * @throws YarnException if the call to the state store is unsuccessful
   */
  public void updateApplicationHomeSubCluster(
      ApplicationHomeSubCluster appHomeSubCluster) throws YarnException {
    stateStore.updateApplicationHomeSubCluster(
        UpdateApplicationHomeSubClusterRequest.newInstance(appHomeSubCluster));
  }

  /**
   * Returns the home {@link SubClusterId} for the specified
   * {@link ApplicationId}.
   *
   * @param appId the identifier of the application
   * @return the home sub cluster identifier
   * @throws YarnException if the call to the state store is unsuccessful
   */
  public SubClusterId getApplicationHomeSubCluster(ApplicationId appId)
      throws YarnException {
    try {
      if (federationCache.isCachingEnabled()) {
        return federationCache.getApplicationHomeSubCluster(appId);
      } else {
        GetApplicationHomeSubClusterResponse response =
            stateStore.getApplicationHomeSubCluster(
                GetApplicationHomeSubClusterRequest.newInstance(appId));
        return response.getApplicationHomeSubCluster().getHomeSubCluster();
      }
    } catch (Throwable ex) {
      throw new YarnException(ex);
    }
  }

  /**
   * Get the {@code ApplicationHomeSubCluster} list representing the mapping of
   * all submitted applications to it's home sub-cluster.
   *
   * @return the mapping of all submitted application to it's home sub-cluster
   * @throws YarnException if the request is invalid/fails
   */
  public List<ApplicationHomeSubCluster> getApplicationsHomeSubCluster()
      throws YarnException {
    GetApplicationsHomeSubClusterResponse response =
        stateStore.getApplicationsHomeSubCluster(
            GetApplicationsHomeSubClusterRequest.newInstance());
    return response.getAppsHomeSubClusters();
  }

  /**
   * Delete the mapping of home {@code SubClusterId} of a previously submitted
   * {@code ApplicationId}. Currently response is empty if the operation is
   * successful, if not an exception reporting reason for a failure.
   *
   * @param applicationId the application to delete the home sub-cluster of
   * @throws YarnException if the request is invalid/fails
   */
  public void deleteApplicationHomeSubCluster(ApplicationId applicationId)
      throws YarnException {
    stateStore.deleteApplicationHomeSubCluster(
        DeleteApplicationHomeSubClusterRequest.newInstance(applicationId));
  }

  /**
   * Get the singleton instance of SubClusterResolver.
   *
   * @return SubClusterResolver instance
   */
  public SubClusterResolver getSubClusterResolver() {
    return this.subclusterResolver;
  }

  /**
   * Get the configuration.
   *
   * @return configuration object
   */
  public Configuration getConf() {
    return this.conf;
  }

  /**
   * Helper method to create instances of Object using the class name defined in
   * the configuration object. The instances creates {@link RetryProxy} using
   * the specific {@link RetryPolicy}.
   *
   * @param conf the yarn configuration
   * @param configuredClassName the configuration provider key
   * @param defaultValue the default implementation for fallback
   * @param type the class for which a retry proxy is required
   * @param retryPolicy the policy for retrying method call failures
   * @return a retry proxy for the specified interface
   */
  public static <T> Object createRetryInstance(Configuration conf,
      String configuredClassName, String defaultValue, Class<T> type,
      RetryPolicy retryPolicy) {

    return RetryProxy.create(type,
        createInstance(conf, configuredClassName, defaultValue, type),
        retryPolicy);
  }

  /**
   * Helper method to create instances of Object using the class name specified
   * in the configuration object.
   *
   * @param conf the yarn configuration
   * @param configuredClassName the configuration provider key
   * @param defaultValue the default implementation class
   * @param type the required interface/base class
   * @param <T> The type of the instance to create
   * @return the instances created
   */
  @SuppressWarnings("unchecked")
  public static <T> T createInstance(Configuration conf,
      String configuredClassName, String defaultValue, Class<T> type) {

    String className = conf.get(configuredClassName, defaultValue);
    try {
      Class<?> clusterResolverClass = conf.getClassByName(className);
      if (type.isAssignableFrom(clusterResolverClass)) {
        return (T) ReflectionUtils.newInstance(clusterResolverClass, conf);
      } else {
        throw new YarnRuntimeException("Class: " + className
            + " not instance of " + type.getCanonicalName());
      }
    } catch (ClassNotFoundException e) {
      throw new YarnRuntimeException("Could not instantiate : " + className, e);
    }
  }

  @VisibleForTesting
  public FederationCache getFederationCache() {
    return federationCache;
  }
}
