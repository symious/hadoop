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

package org.apache.hadoop.yarn.server.router.rmadmin;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshQueuesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshQueuesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshSuperUserGroupsConfigurationRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshSuperUserGroupsConfigurationResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshUserToGroupsMappingsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshUserToGroupsMappingsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshServiceAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshServiceAclsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesResourcesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshNodesResourcesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoveFromClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoveFromClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReplaceLabelsOnNodeRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReplaceLabelsOnNodeResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.CheckForDecommissioningNodesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.CheckForDecommissioningNodesResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshClusterMaxPriorityRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshClusterMaxPriorityResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodesToAttributesMappingRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodesToAttributesMappingResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.DeleteFederationApplicationRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.DeleteFederationApplicationResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateRMConfigRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateRMConfigResponse;
import org.apache.hadoop.yarn.server.federation.failover.FederationProxyProviderUtil;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.router.RouterMetrics;
import org.apache.hadoop.yarn.server.router.RouterServerUtil;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.MonotonicClock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;

public class FederationRMAdminInterceptor extends AbstractRMAdminRequestInterceptor {

  private static final Logger LOG =
      LoggerFactory.getLogger(FederationRMAdminInterceptor.class);

  private static final String COMMA = ",";
  private static final String COLON = ":";

  private Map<SubClusterId, ResourceManagerAdministrationProtocol> adminRMProxies;
  private FederationStateStoreFacade federationFacade;
  private final Clock clock = new MonotonicClock();
  private RouterMetrics routerMetrics;
  private ThreadPoolExecutor executorService;
  private Configuration conf;

  @Override
  public void init(String userName) {
    super.init(userName);

    int numThreads = getConf().getInt(
        YarnConfiguration.ROUTER_USER_CLIENT_THREADS_SIZE,
        YarnConfiguration.DEFAULT_ROUTER_USER_CLIENT_THREADS_SIZE);
    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setNameFormat("RPC Router RMAdminClient-" + userName + "-%d ").build();

    long keepAliveTime = getConf().getTimeDuration(
        YarnConfiguration.ROUTER_USER_CLIENT_THREAD_POOL_KEEP_ALIVE_TIME,
        YarnConfiguration.DEFAULT_ROUTER_USER_CLIENT_THREAD_POOL_KEEP_ALIVE_TIME, TimeUnit.SECONDS);

    BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>();
    this.executorService = new ThreadPoolExecutor(numThreads, numThreads,
        keepAliveTime, TimeUnit.MILLISECONDS, workQueue, threadFactory);

    federationFacade = FederationStateStoreFacade.getInstance();
    this.conf = this.getConf();
    this.adminRMProxies = new ConcurrentHashMap<>();
    routerMetrics = RouterMetrics.getMetrics();
  }

  @VisibleForTesting
  protected ResourceManagerAdministrationProtocol getAdminRMProxyForSubCluster(
      SubClusterId subClusterId) throws Exception {

    if (adminRMProxies.containsKey(subClusterId)) {
      return adminRMProxies.get(subClusterId);
    }

    ResourceManagerAdministrationProtocol adminRMProxy = null;
    try {
      boolean serviceAuthEnabled = this.conf.getBoolean(
          CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false);
      UserGroupInformation realUser = user;
      if (serviceAuthEnabled) {
        realUser = UserGroupInformation.createProxyUser(
            user.getShortUserName(), UserGroupInformation.getLoginUser());
      }
      adminRMProxy = FederationProxyProviderUtil.createRMProxy(getConf(),
          ResourceManagerAdministrationProtocol.class, subClusterId, realUser);
    } catch (Exception e) {
      RouterServerUtil.logAndThrowException(
          "Unable to create the interface to reach the SubCluster " +
              subClusterId, e);
    }
    adminRMProxies.put(subClusterId, adminRMProxy);
    return adminRMProxy;
  }

  @Override
  public void setNextInterceptor(RMAdminRequestInterceptor next) {
    throw new YarnRuntimeException("setNextInterceptor is being called on "
        + "FederationRMAdminRequestInterceptor, which should be the last one "
        + "in the chain. Check if the interceptor pipeline configuration "
        + "is correct");
  }


  @VisibleForTesting
  public FederationStateStoreFacade getFederationFacade() {
    return federationFacade;
  }

  @VisibleForTesting
  public ThreadPoolExecutor getExecutorService() {
    return executorService;
  }

  @Override
  public RefreshQueuesResponse refreshQueues(RefreshQueuesRequest request)
      throws StandbyException, YarnException, IOException {
    throw new NotImplementedException("Code is not implemented");
  }

  @Override
  public RefreshNodesResponse refreshNodes(RefreshNodesRequest request)
      throws StandbyException, YarnException, IOException {
    throw new NotImplementedException("Code is not implemented");
  }

  @Override
  public RefreshSuperUserGroupsConfigurationResponse refreshSuperUserGroupsConfiguration(
      RefreshSuperUserGroupsConfigurationRequest request)
      throws StandbyException, YarnException, IOException {
    throw new NotImplementedException("Code is not implemented");
  }

  @Override
  public RefreshUserToGroupsMappingsResponse refreshUserToGroupsMappings(
      RefreshUserToGroupsMappingsRequest request)
      throws StandbyException, YarnException, IOException {
    throw new NotImplementedException("Code is not implemented");
  }

  @Override
  public RefreshAdminAclsResponse refreshAdminAcls(
      RefreshAdminAclsRequest request) throws YarnException, IOException {
    throw new NotImplementedException("Code is not implemented");
  }

  @Override
  public RefreshServiceAclsResponse refreshServiceAcls(
      RefreshServiceAclsRequest request) throws YarnException, IOException {
    throw new NotImplementedException("Code is not implemented");
  }

  @Override
  public UpdateRMConfigResponse updateRMConfig(UpdateRMConfigRequest request)
      throws YarnException, IOException {
    throw new NotImplementedException("Code is not implemented");
  }

  @Override
  public UpdateNodeResourceResponse updateNodeResource(
      UpdateNodeResourceRequest request) throws YarnException, IOException {
    throw new NotImplementedException("Code is not implemented");
  }

  @Override
  public RefreshNodesResourcesResponse refreshNodesResources(
      RefreshNodesResourcesRequest request) throws YarnException, IOException {
    throw new NotImplementedException("Code is not implemented");
  }

  @Override
  public AddToClusterNodeLabelsResponse addToClusterNodeLabels(
      AddToClusterNodeLabelsRequest request) throws YarnException, IOException {
    throw new NotImplementedException("Code is not implemented");
  }

  @Override
  public RemoveFromClusterNodeLabelsResponse removeFromClusterNodeLabels(
      RemoveFromClusterNodeLabelsRequest request)
      throws YarnException, IOException {
    throw new NotImplementedException("Code is not implemented");
  }

  @Override
  public ReplaceLabelsOnNodeResponse replaceLabelsOnNode(
      ReplaceLabelsOnNodeRequest request) throws YarnException, IOException {
    throw new NotImplementedException("Code is not implemented");
  }

  @Override
  public CheckForDecommissioningNodesResponse checkForDecommissioningNodes(
      CheckForDecommissioningNodesRequest checkForDecommissioningNodesRequest)
      throws YarnException, IOException {
    throw new NotImplementedException("Code is not implemented");
  }

  @Override
  public RefreshClusterMaxPriorityResponse refreshClusterMaxPriority(
      RefreshClusterMaxPriorityRequest request)
      throws YarnException, IOException {
    throw new NotImplementedException("Code is not implemented");
  }

  @Override
  public NodesToAttributesMappingResponse mapAttributesToNodes(
      NodesToAttributesMappingRequest request)
      throws YarnException, IOException {
    throw new NotImplementedException("Code is not implemented");
  }

  @Override
  public DeleteFederationApplicationResponse deleteFederationApplication(
      DeleteFederationApplicationRequest request) throws YarnException {

    // Parameter validation.
    if (request == null) {
      routerMetrics.incrDeleteFederationApplicationFailedRetrieved();
      RouterServerUtil.logAndThrowException(
          "Missing deleteFederationApplication Request.", null);
    }

    String application = request.getApplication();
    String subClusterId = request.getSubClusterId();

    if (StringUtils.isBlank(application) && StringUtils.isBlank(subClusterId)) {
      routerMetrics.incrDeleteFederationApplicationFailedRetrieved();
      RouterServerUtil.logAndThrowException(
          "ApplicationId and subClusterId cannot both be null.", null);
    }

    if (StringUtils.isNotBlank(subClusterId)) {
      return deleteFederationApplicationByClusterId(subClusterId);
    }

    if (StringUtils.isNotBlank(application)) {
      return deleteFederationApplicationByAppId(application);
    }

    throw new YarnException("Unable to deleteFederationApplication.");
  }

  public DeleteFederationApplicationResponse deleteFederationApplicationByAppId(
      String application) throws YarnException {
    try {
      long startTime = clock.getTime();
      ApplicationId applicationId = ApplicationId.fromString(application);
      federationFacade.deleteApplicationHomeSubCluster(applicationId);
      long stopTime = clock.getTime();
      routerMetrics.succeededDeleteFederationApplicationFailedRetrieved(
          stopTime - startTime);
      return DeleteFederationApplicationResponse.newInstance(
          "applicationId = " + applicationId + " delete success.");
    } catch (Exception e) {
      RouterServerUtil.logAndThrowException(
          "Unable to deleteFederationApplication due to exception. " +
              e.getMessage(), e);
    }
    throw new YarnException("Unable to deleteFederationApplication.");
  }

  public DeleteFederationApplicationResponse deleteFederationApplicationByClusterId(
      String subClusterId) throws YarnException {
    try {
      long startTime = clock.getTime();

      List<ApplicationHomeSubCluster> applicationHomeSubClusterList =
          federationFacade.getApplicationsHomeSubCluster();
      int deletedApps = 0;

      for (ApplicationHomeSubCluster app : applicationHomeSubClusterList) {
        ApplicationId applicationId = app.getApplicationId();
        SubClusterId homeSubCluster = app.getHomeSubCluster();
        if (homeSubCluster.toString().equals(subClusterId)) {
          federationFacade.deleteApplicationHomeSubCluster(applicationId);
          deletedApps++;
        }
      }

      long stopTime = clock.getTime();
      routerMetrics.succeededDeleteFederationApplicationFailedRetrieved(
          stopTime - startTime);
      return DeleteFederationApplicationResponse.newInstance(
          "Delete " + deletedApps + " apps by subClusterId = " + subClusterId +
              " success.");
    } catch (Exception e) {
      RouterServerUtil.logAndThrowException(
          "Unable to deleteFederationApplication due to exception. " +
              e.getMessage(), e);
    }
    throw new YarnException("Unable to deleteFederationApplication.");
  }

  @Override
  public String[] getGroupsForUser(String user) throws IOException {
    return new String[0];
  }
}
