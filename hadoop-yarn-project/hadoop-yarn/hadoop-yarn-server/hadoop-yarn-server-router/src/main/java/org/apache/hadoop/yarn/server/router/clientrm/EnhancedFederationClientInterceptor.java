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

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.client.urlconnection.HttpURLConnectionFactory;
import com.sun.jersey.client.urlconnection.URLConnectionClientHandler;
import org.apache.hadoop.ipc.Server;

import org.apache.commons.lang3.RandomStringUtils;

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
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.utils.CacheUtil;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.metrics.ApplicationMetricsConstants;
import org.apache.hadoop.yarn.server.router.RouterMetrics;
import org.apache.hadoop.yarn.server.router.RouterServerUtil;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.MonotonicClock;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.webapp.YarnJacksonJaxbJsonProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

  private static final String GET_CLUSTER_NODES_CACHEID = "getClusterNodes";
  private static final String TIC_TAG_PREFIX = "tic:";
  private static final String LIVY_TAG_PREFIX = "livy:";

  public static final int HTTP_CLIENT_TIMEOUT = 30000;

  public EnhancedFederationClientInterceptor() {
    federationFacade = FederationStateStoreFacade.getInstance();
    routerMetrics = RouterMetrics.getMetrics();
  }

  public boolean enableQueryTimeLine(){
    return getConf().getBoolean(YarnConfiguration.ROUTER_QUERY_TIMELINE_ENABLED,
        YarnConfiguration.DEFAULT_ROUTER_QUERY_TIMELINE_ENABLED);
  }

  public String getQueryTimeLineAddress(){
    return getConf().get(YarnConfiguration.GPG_QUERY_TIMELINE_WEBAPP_ADDRESS,
        YarnConfiguration.DEFAULT_GPG_QUERY_TIMELINE_WEBAPP_ADDRESS);
  }

  public static ClientResponse getResponse(Client client, URI uri)
      throws Exception {
    ClientResponse resp =
        client.resource(uri).accept(MediaType.APPLICATION_JSON)
            .type(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    if (resp == null ||
        resp.getStatusInfo().getStatusCode() !=
            ClientResponse.Status.OK.getStatusCode()) {
      String msg = new String();
      if (resp != null) {
        msg = String.valueOf(resp.getStatusInfo().getStatusCode());
      }
      throw new IOException("Incorrect response from timeline reader. " +
          "Status=" + msg);
    }
    return resp;
  }

  public static Client createClient() {
    ClientConfig cfg = new DefaultClientConfig();
    cfg.getClasses().add(YarnJacksonJaxbJsonProvider.class);
    return new Client(new URLConnectionClientHandler(
        new DummyURLConnectionFactory()), cfg);
  }

  private static class DummyURLConnectionFactory
      implements HttpURLConnectionFactory {

    @Override
    public HttpURLConnection getHttpURLConnection(final URL url)
        throws IOException {
      try {
        HttpURLConnection httpURLConnection = (HttpURLConnection)url.openConnection();
        httpURLConnection.setConnectTimeout(HTTP_CLIENT_TIMEOUT);
        httpURLConnection.setReadTimeout(HTTP_CLIENT_TIMEOUT);
        return httpURLConnection;
      } catch (UndeclaredThrowableException e) {
        throw new IOException(e.getCause());
      }
    }
  }

  public ApplicationReport queryAppsByTagFromTimeLine(String tagName) {

    int index = tagName.indexOf(":");
    String queryTimelineAddress = getQueryTimeLineAddress();
    String queryAppIdByTagUrl =
        queryTimelineAddress + "/ws/v2/timeline/tics/" +
            tagName.substring(index + 1);
    if (LOG.isDebugEnabled()) {
      LOG.debug("timeline queryAppIdByTagUrl: " + queryAppIdByTagUrl);
    }

    ClientResponse queryAppIdByTagResp = null;
    ClientResponse queryAppStateResp = null;
    String appId = "";
    Set<String> appTags = new HashSet<>();
    Client httpClient = createClient();
    URI queryAppIdByTagUri = URI.create(queryAppIdByTagUrl);

    try {
      queryAppIdByTagResp = getResponse(httpClient, queryAppIdByTagUri);
      appId = queryAppIdByTagResp.getEntity(String.class).replaceAll("\"", "");
    } catch (Exception e) {
      LOG.error("timeline queryAppIdByTag failed", e);
    } finally {
      if (queryAppIdByTagResp != null) {
        queryAppIdByTagResp.close();
      }
    }

    if (!appId.isEmpty()) {
      String queryAppStatesUrl =
          queryTimelineAddress + "/ws/v2/timeline/apps/" + appId + "?fields=ALL";
      if (LOG.isDebugEnabled()) {
        LOG.debug("timeline queryAppStatesUrl: " + queryAppStatesUrl);
      }
      URI queryAppStatesUri = URI.create(queryAppStatesUrl);
      try {
        queryAppStateResp = getResponse(httpClient, queryAppStatesUri);
        TimelineEntity entity = queryAppStateResp.getEntity(TimelineEntity.class);
        Map<String, Object> entityInfo = entity.getInfo();
        if (entityInfo.containsKey(ApplicationMetricsConstants.APP_TAGS_INFO)) {
          Object obj = entityInfo.get(ApplicationMetricsConstants.APP_TAGS_INFO);
          if (obj != null && obj instanceof Collection<?>) {
            for(Object o : (Collection<?>)obj) {
              if (o != null) {
                appTags.add(o.toString());
              }
            }
          }
        }
      } catch (Exception e) {
        LOG.error("timeline query appId: " + appId + " states failed", e);
      } finally {
        if (queryAppStateResp != null) {
          queryAppStateResp.close();
        }
        if (httpClient != null) {
          httpClient.destroy();
        }
      }
    }

    ApplicationReport appReport = Records.newRecord(ApplicationReport.class);
    if (!appId.isEmpty()) {
      appReport.setApplicationId(ApplicationId.fromString(appId));
      appReport.setApplicationTags(appTags);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Timeline getApplications: appId: " + appId + " ,appTags: " +
            appTags);
      }
    }
    return appReport;
  }

  @Override
  public GetApplicationsResponse getApplications(GetApplicationsRequest request)
      throws YarnException, IOException {

    if (request == null) {
      routerMetrics.incrMultipleAppsFailedRetrieved();
      RouterServerUtil.logAndThrowException("Missing getApplications request.",
          null);
    }

    long startTime = clock.getTime();
    String requestId = RandomStringUtils.randomAlphabetic(8);

    Set<String> queryTags = request.getApplicationTags();
    LOG.info("requestId: " + requestId + " ,getApplications request info -> " +
        "tags: " + queryTags + ", states: " + request.getApplicationStates() +
        ", types: " + request.getApplicationTypes() + ", limit: "
        + request.getLimit() + ", clientIP: " + Server.getRemoteAddress());

    if (queryTags == null || queryTags.size() <= 0) {
      RouterServerUtil.logAndThrowException(
          "Router does not support getApplications requests without specifying a tag!",
          null);
    }

    GetApplicationsResponse getApplicationsResponse;
    List<ApplicationReport> applications = new ArrayList<>();
    Set<String> queryFromTimeLineTags = new HashSet<>();
    Set<String> queryFromYarnTags = new HashSet<>();

    //query apps from timeline
    long startTime1 = clock.getTime();
    for (String tag : queryTags) {
      if (enableQueryTimeLine() && (tag.startsWith(TIC_TAG_PREFIX) ||
          tag.startsWith(LIVY_TAG_PREFIX))) {
        ApplicationReport appReport = queryAppsByTagFromTimeLine(tag);
        if (appReport != null && appReport.getApplicationId() != null &&
            !appReport.getApplicationId().toString().isEmpty() &&
            appReport.getApplicationTags() != null &&
            appReport.getApplicationTags().size() > 0) {
          applications.add(appReport);
          queryFromTimeLineTags.add(tag);
        } else {
          queryFromYarnTags.add(tag);
        }
      } else {
        queryFromYarnTags.add(tag);
      }
    }
    long stopTime1 = clock.getTime();
    if (queryFromTimeLineTags.size() > 0) {
      LOG.info("requestId: " + requestId + " ,getApplications, tags: " +
          queryFromTimeLineTags + " ,from TimeLineService cost time: " +
          (stopTime1 - startTime1) + "ms, clientIP: " +
          Server.getRemoteAddress());
    }

    //query apps from yarn
    if (queryFromYarnTags.size() > 0) {
      long startTime2 = clock.getTime();
      request.setApplicationTags(queryFromYarnTags);
      Map<SubClusterId, SubClusterInfo> subclusters =
          federationFacade.getSubClusters(true);
      ClientMethod remoteMethod = new ClientMethod("getApplications",
          new Class[] {GetApplicationsRequest.class}, new Object[] {request});
      ArrayList<SubClusterId> clusterList =
          new ArrayList<>(subclusters.keySet());
      Map<SubClusterId, GetApplicationsResponse> clusterApps =
          invokeConcurrent(clusterList, remoteMethod,
              GetApplicationsResponse.class, requestId);
      long stopTime2 = clock.getTime();
      applications
          .addAll(RouterYarnClientUtils.mergeApps(clusterApps.values()));
      LOG.info("requestId: " + requestId + " ,getApplications, tags: " +
          queryFromYarnTags + " ,from YARN cost time: " +
          (stopTime2 - startTime2) + "ms, clientIP: " +
          Server.getRemoteAddress());
    }

    getApplicationsResponse = GetApplicationsResponse.newInstance(applications);
    long stopTime = clock.getTime();
    LOG.info("requestId: " + requestId + " ,getApplications, tags: " +
        queryTags + " ,sum cost time: " + (stopTime - startTime) +
        "ms, clientIP: " + Server.getRemoteAddress() + " ,result size: " +
        applications.size());
    if (applications.size() > 0) {
      routerMetrics.succeededMultipleAppsRetrieved(stopTime - startTime);
    }
    return getApplicationsResponse;
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

    String requestId = RandomStringUtils.randomAlphabetic(8);
    LOG.info("requestId:" + requestId +
        " ,getClusterNodes request info -> nodeStates: " +
        request.getNodeStates());

    Map<SubClusterId, SubClusterInfo> subClusters =
        federationFacade.getSubClusters(true);
    ArrayList<SubClusterId> clusterList = new ArrayList<>(subClusters.keySet());
    ClientMethod remoteMethod = new ClientMethod("getClusterNodes",
        new Class[] {GetClusterNodesRequest.class}, new Object[] {request});

    Map<SubClusterId, GetClusterNodesResponse> clusterNodes;

    if (routerRpcRequestCache.isCachingEnabled()) {
      long startTime = clock.getTime();
      clusterNodes = (Map<SubClusterId, GetClusterNodesResponse>) cache
          .get(buildGetClusterNodesCacheRequest(clusterList, remoteMethod,
              GetClusterNodesResponse.class, requestId));
      for (Map.Entry<SubClusterId, GetClusterNodesResponse> entry : clusterNodes
          .entrySet()) {
        SubClusterId key = entry.getKey();
        GetClusterNodesResponse value = entry.getValue();
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              "requestId:" + requestId + " ,SubClusterId: " + key.toString() +
                  " ,NumNodeReports: " +
                  value.getNodeReports().size());
        }
      }
      long stopTime = clock.getTime();
      LOG.info("requestId:" + requestId +
          " ,getClusterNodes from cache cost time: " + (stopTime - startTime) +
          "ms, clientIP: " + Server.getRemoteAddress());
    } else {
      long startTime = clock.getTime();
      clusterNodes = invokeConcurrent(clusterList, remoteMethod,
          GetClusterNodesResponse.class, requestId);
      long stopTime = clock.getTime();
      LOG.info("requestId:" + requestId +
          " ,getClusterNodes without cache cost time: " +
          (stopTime - startTime) + "ms, clientIP: " +
          Server.getRemoteAddress());
    }

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

  private Object buildGetClusterNodesCacheRequest(
      ArrayList<SubClusterId> clusterList, ClientMethod remoteMethod,
      Class clazz, String requestId) {
    final String cacheKey =
        buildCacheKey(getClass().getSimpleName(), GET_CLUSTER_NODES_CACHEID,
            remoteMethod.getParams()[0].toString());
    LOG.info("buildGetClusterNodesCacheRequest cacheKey: " + cacheKey);
    CacheUtil.CacheRequest<String, Map<SubClusterId, GetClusterNodesResponse>>
        cacheRequest =
        new CacheUtil.CacheRequest<String, Map<SubClusterId, GetClusterNodesResponse>>(
            cacheKey,
            new CacheUtil.Func<String, Map<SubClusterId, GetClusterNodesResponse>>() {
              @Override
              public Map<SubClusterId, GetClusterNodesResponse> invoke(
                  String key)
                  throws Exception {
                Map<SubClusterId, GetClusterNodesResponse> clusterNodes =
                    invokeConcurrent(clusterList, remoteMethod, clazz, requestId);
                LOG.info("buildGetClusterNodesCacheRequest refresh!");
                return clusterNodes;
              }
            });
    return cacheRequest;
  }
}
