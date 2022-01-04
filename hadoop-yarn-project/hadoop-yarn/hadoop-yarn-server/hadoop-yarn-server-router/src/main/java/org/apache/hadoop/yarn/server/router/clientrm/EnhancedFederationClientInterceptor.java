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

package org.apache.hadoop.yarn.server.router.clientrm;

import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.router.RouterMetrics;
import org.apache.hadoop.yarn.server.router.RouterServerUtil;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.MonotonicClock;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A custom class to Add the missing FederationClientInterceptor method
 * SPDI-9345
 */
public class EnhancedFederationClientInterceptor
    extends FederationClientInterceptor {

  private static final Logger LOG =
      LoggerFactory.getLogger(EnhancedFederationClientInterceptor.class);

  private FederationStateStoreFacade federationFacade;
  private RouterMetrics routerMetrics;
  private final Clock clock = new MonotonicClock();

  public EnhancedFederationClientInterceptor() {
    federationFacade = FederationStateStoreFacade.getInstance();
    routerMetrics = RouterMetrics.getMetrics();
  }

  @Override
  public GetApplicationsResponse getApplications(GetApplicationsRequest request)
      throws YarnException, IOException {

    long startTime = clock.getTime();

    if (request == null) {
      routerMetrics.incrMultipleAppsFailedRetrieved();
      RouterServerUtil.logAndThrowException("Missing getApplications request.",
          null);
    }

    LOG.info("GetApplications request info -> tags: " +
        request.getApplicationTags() + ", states: " +
        request.getApplicationStates() + ", types: " +
        request.getApplicationTypes() + ", limit: " + request.getLimit());

    if (request.getApplicationTags() == null ||
        request.getApplicationTags().size() <= 0) {
      RouterServerUtil.logAndThrowException(
          "Router does not support getApplications requests without specifying a tag!",
          null);
    }

    Map<SubClusterId, SubClusterInfo> subclusters =
        federationFacade.getSubClusters(true);
    ClientMethod remoteMethod = new ClientMethod("getApplications",
        new Class[] {GetApplicationsRequest.class}, new Object[] {request});
    ArrayList<SubClusterId> clusterList = new ArrayList<>(subclusters.keySet());
    Map<SubClusterId, GetApplicationsResponse> clusterApps =
        invokeConcurrent(clusterList, remoteMethod,
            GetApplicationsResponse.class);

    long stopTime = clock.getTime();
    if (clusterApps.size() > 0) {
      routerMetrics.succeededMultipleAppsRetrieved(stopTime - startTime);
    }
    LOG.info("GetApplications cost time: " + (stopTime - startTime) + "ms");
    return RouterYarnClientUtils.mergeApps(clusterApps.values());
  }

  @Override
  public GetContainerReportResponse getContainerReport(
      GetContainerReportRequest request) throws YarnException, IOException {
    long startTime = clock.getTime();

    if (request == null) {
      RouterServerUtil.logAndThrowException("Missing getContainerReport " +
          "request.", null);
    }

    ContainerId containerId = request.getContainerId();
    if (containerId == null) {
      RouterServerUtil.logAndThrowException("ContainerReportRequest miss " +
          "containerId information.", null);
    }

    ApplicationAttemptId applicationAttemptId =
        containerId.getApplicationAttemptId();
    ApplicationId applicationId = applicationAttemptId.getApplicationId();

    SubClusterId subClusterId = null;

    try {
      subClusterId = federationFacade
          .getApplicationHomeSubCluster(applicationId);
    } catch (YarnException e) {
      RouterServerUtil
          .logAndThrowException("Application " + applicationId
              + " does not exist in FederationStateStore", e);
    }

    ApplicationClientProtocol clientRMProxy =
        getClientRMProxyForSubCluster(subClusterId);

    GetContainerReportResponse response = null;
    try {
      acquirePermit(subClusterId.getId());
      response = clientRMProxy.getContainerReport(request);
    } catch (Exception e) {
      LOG.error("Unable to get the container report for "
          + applicationId + "to SubCluster "
          + subClusterId.getId(), e);
      throw e;
    } finally {
      releasePermit(subClusterId.getId());
    }

    if (response == null) {
      LOG.error("No response when attempting to retrieve the report of "
          + "the containerId " + containerId + " to SubCluster "
          + subClusterId.getId());
    } else {
      ContainerReport containerReport = response.getContainerReport();
      ContainerState containerState = containerReport.getContainerState();
      LOG.debug(
          "ContainerId: " + containerId.toString() + ", ContainerState: " +
              containerState);
    }

    long stopTime = clock.getTime();
    LOG.debug("GetContainerReport cost time: " + (stopTime - startTime) + "ms");
    return response;
  }

  @Override
  public GetClusterNodesResponse getClusterNodes(GetClusterNodesRequest request)
      throws YarnException, IOException {

    long startTime = clock.getTime();

    LOG.info("GetClusterNodes request info -> nodeStates: "
        + request.getNodeStates());

    Map<SubClusterId, SubClusterInfo> subclusters =
        federationFacade.getSubClusters(true);
    ClientMethod remoteMethod = new ClientMethod("getClusterNodes",
        new Class[] {GetClusterNodesRequest.class}, new Object[] {request});
    ArrayList<SubClusterId> clusterList = new ArrayList<>(subclusters.keySet());
    Map<SubClusterId, GetClusterNodesResponse> clusterNodes =
        invokeConcurrent(clusterList, remoteMethod,
            GetClusterNodesResponse.class);

    long stopTime = clock.getTime();
    LOG.info("GetClusterNodes cost time: " + (stopTime - startTime) + "ms");
    return RouterYarnClientUtils.mergeNodes(clusterNodes.values());
  }

  @Override
  public GetQueueInfoResponse getQueueInfo(GetQueueInfoRequest request)
      throws YarnException, IOException {

    long startTime = clock.getTime();

    QueueInfo queueInfo = null;

    LOG.info("GetQueueInfo request info -> QueueName: " +
        request.getQueueName() + ", IncludeApplications: " +
        request.getIncludeApplications() + ", IncludeChildQueues: " +
        request.getIncludeChildQueues() + ", Recursive: " +
        request.getRecursive());

    Map<SubClusterId, SubClusterInfo> subClustersActive =
        federationFacade.getSubClusters(true);

    for(Map.Entry<SubClusterId, SubClusterInfo> entry :
        subClustersActive.entrySet()){
      SubClusterId subClusterId = entry.getKey();
      ApplicationClientProtocol clientRMProxy =
          getClientRMProxyForSubCluster(subClusterId);
      GetQueueInfoResponse response = null;
      try {
        acquirePermit(subClusterId.getId());
        response = clientRMProxy.getQueueInfo(request);
      } catch (Exception e) {
        LOG.warn("Unable to getQueueInfo in SubCluster "
            + subClusterId.getId(), e);
      } finally {
        releasePermit(subClusterId.getId());
      }

      if (response != null) {
        QueueInfo tmpQueueInfo = response.getQueueInfo();
        LOG.info("GetQueueInfo response info -> QueueName: " +
            tmpQueueInfo.getQueueName());
        if (tmpQueueInfo.getQueueName() != null &&
            request.getQueueName().equals(tmpQueueInfo.getQueueName())) {
          queueInfo = tmpQueueInfo;
          long stopTime = clock.getTime();
          LOG.info(
              "Get cluster queueInfo from cluster [" + subClusterId + "]," +
                  "cost time: " + (stopTime - startTime) + "ms");
          break;
        }
      }
    }

    GetQueueInfoResponse response =
        Records.newRecord(GetQueueInfoResponse.class);
    response.setQueueInfo(queueInfo);
    return response;
  }

  @Override
  public GetApplicationAttemptsResponse getApplicationAttempts(
      GetApplicationAttemptsRequest request) throws YarnException, IOException {

    long startTime = clock.getTime();

    if (request == null || request.getApplicationId() == null) {
      routerMetrics.incrAppAttemptsFailedRetrieved();
      RouterServerUtil.logAndThrowException(
          "Missing getApplicationAttempts request or applicationId or " +
              "information.", null);
    }

    SubClusterId subClusterId = null;

    try {
      subClusterId = federationFacade
          .getApplicationHomeSubCluster(request.getApplicationId());
    } catch (YarnException e) {
      routerMetrics.incrAppAttemptsFailedRetrieved();
      RouterServerUtil
          .logAndThrowException("Application " + request.getApplicationId() +
              " does not exist in FederationStateStore", e);
    }

    ApplicationClientProtocol clientRMProxy =
        getClientRMProxyForSubCluster(subClusterId);

    GetApplicationAttemptsResponse response = null;
    try {
      acquirePermit(subClusterId.getId());
      response = clientRMProxy.getApplicationAttempts(request);
    } catch (Exception e) {
      routerMetrics.incrAppAttemptsFailedRetrieved();
      LOG.error("Unable to getApplicationAttempts for " +
          request.getApplicationId() + "to SubCluster " + subClusterId.getId(), e);
      throw e;
    } finally {
      releasePermit(subClusterId.getId());
    }

    if (response == null) {
      LOG.error("No response when attempting to retrieve applicationAttempts "
          + request.getApplicationId() + " to SubCluster "
          + subClusterId.getId());
    }

    long stopTime = clock.getTime();
    routerMetrics.succeededAppAttemptsRetrieved(stopTime - startTime);
    return response;
  }

  @Override
  public GetContainersResponse getContainers(GetContainersRequest request)
      throws YarnException, IOException {
    long startTime = clock.getTime();

    if (request == null) {
      RouterServerUtil.logAndThrowException("Missing getContainers request.",
          null);
    }

    ApplicationAttemptId applicationAttemptId =
        request.getApplicationAttemptId();
    ApplicationId applicationId = applicationAttemptId.getApplicationId();

    SubClusterId subClusterId = null;

    try {
      subClusterId = federationFacade
          .getApplicationHomeSubCluster(applicationId);
    } catch (YarnException e) {
      RouterServerUtil
          .logAndThrowException("Application " + applicationId
              + " does not exist in FederationStateStore", e);
    }

    ApplicationClientProtocol clientRMProxy =
        getClientRMProxyForSubCluster(subClusterId);

    GetContainersResponse response = null;
    try {
      acquirePermit(subClusterId.getId());
      response = clientRMProxy.getContainers(request);
    } catch (Exception e) {
      LOG.error("Unable to getContainers for " + applicationId + "to " +
          "SubCluster " + subClusterId.getId(), e);
      throw e;
    } finally {
      releasePermit(subClusterId.getId());
    }

    if (response == null) {
      LOG.error("No response when attempting to retrieve containers of "
          + "the applicationId " + applicationId + " to SubCluster "
          + subClusterId.getId());
    } else {
      List<ContainerReport> containerReportList = response.getContainerList();
      if (LOG.isDebugEnabled()) {
        for (ContainerReport containerReport : containerReportList) {
          LOG.debug("ContainerId: " + containerReport.getContainerId() +
              ", ContainerState: " + containerReport.getContainerState());
        }
      }
    }

    long stopTime = clock.getTime();
    LOG.debug("getContainers cost time: " + (stopTime - startTime) + "ms");
    return response;
  }
}
