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

package org.apache.hadoop.yarn.server.globalpolicygenerator.webapp;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.policies.manager.WeightedLocalityPolicyManager;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterIdInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.globalpolicygenerator.GlobalPolicyGenerator;
import org.apache.hadoop.yarn.server.globalpolicygenerator.webapp.dao.ClusterWeight;
import org.apache.hadoop.yarn.server.globalpolicygenerator.webapp.dao.ClusterWeights;
import org.apache.hadoop.yarn.server.globalpolicygenerator.webapp.dao.GPGInfo;
import org.apache.hadoop.yarn.server.globalpolicygenerator.webapp.dao.PolicyListInfo;
import org.apache.hadoop.yarn.server.globalpolicygenerator.webapp.dao.PolicyRequestsInfo;
import org.apache.hadoop.yarn.server.globalpolicygenerator.webapp.dao.PolicyUpdateRequestInfo;
import org.apache.hadoop.yarn.server.globalpolicygenerator.webapp.dao.PolicyUpdateResponseInfo;
import org.apache.hadoop.yarn.server.globalpolicygenerator.webapp.dao.PolicyRequestInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.DeSelectFields;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppsInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static javax.servlet.http.HttpServletResponse.SC_OK;
import static org.apache.hadoop.yarn.webapp.util.WebAppUtils.getHttpSchemePrefix;

/**
 * GPGWebServices is a service that runs on one GPG that can be used to handle
 * some requests from clients, such as routing rules change, queue resources
 * change, etc
 **/
@Singleton
@Path("/ws/v1/cluster")
public class GPGWebServices{

  private static final Logger LOG =
      LoggerFactory.getLogger(GPGWebServices.class);
  private final GlobalPolicyGenerator gpg;
  private final Configuration conf;
  private @Context HttpServletResponse response;
  private FederationStateStoreFacade federationFacade;

  @Inject
  public GPGWebServices(final GlobalPolicyGenerator gpg, Configuration conf) {
    this.gpg = gpg;
    this.conf = conf;
    this.federationFacade = FederationStateStoreFacade.getInstance();
  }

  @VisibleForTesting
  protected void setResponse(HttpServletResponse response) {
    this.response = response;
  }

  private void init() {
    // clear content type
    response.setContentType(null);
  }

  /**
   * Performs an invocation of the the remote RMWebService.
   */
  public static <T> T invokeRMWebService(String webAddr,
      String path, final Class<T> returnType, String deSelectParam) {
    Client client = Client.create();
    T obj;

    WebResource webResource =
        client.resource(webAddr).path("ws/v1/cluster").path(path);
    if (deSelectParam != null) {
      webResource = webResource.queryParam(RMWSConsts.DESELECTS, deSelectParam);
    }
    ClientResponse response = null;
    try {
      response = webResource.accept(MediaType.APPLICATION_XML)
          .get(ClientResponse.class);
      if (response.getStatus() == SC_OK) {
        obj = response.getEntity(returnType);
      } else {
        throw new YarnRuntimeException(
            "Bad response from remote web service: " + response.getStatus());
      }
      return obj;
    } finally {
      if (response != null) {
        response.close();
      }
      client.destroy();
    }
  }

  @GET
  @Path("/test")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public GPGInfo getGPGInfo(@QueryParam("msg") String msg) {
    init();
    return new GPGInfo(msg);
  }

  @GET
  @Path(GPGWSConsts.POLICY_LIST)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public Response listPolicy(
      @QueryParam(GPGWSConsts.QUEUENAME) String queueName,
      @Context HttpServletRequest hsr) throws Exception {

    init();

    List<SubClusterPolicyConfiguration> spcList = new ArrayList<>();

    if(StringUtils.isNotBlank(queueName)){
      SubClusterPolicyConfiguration spc =
          federationFacade.getPolicyConfiguration(queueName);
      if(spc!=null){
        spcList.add(spc);
      }
    }else{
      Map<String, SubClusterPolicyConfiguration> spcMap =
          federationFacade.getPoliciesConfigurations();
      if(spcMap != null){
        for(Map.Entry<String,SubClusterPolicyConfiguration> entry : spcMap.entrySet()){
          spcList.add(entry.getValue());
        }
      }
    }

    if(spcList.size()==0){
      String msg = "No policy found!";
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(msg).
          build();
    }
    LOG.info("listPolicy= " + spcList);
    PolicyListInfo resResponse = new PolicyListInfo(spcList);
    return Response.status(Response.Status.OK).entity(resResponse).build();
  }

  @POST
  @Path(GPGWSConsts.POLICY_UPDATE)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  @Consumes({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
  public Response updatePolicy(PolicyUpdateRequestInfo resContext,
      @Context HttpServletRequest hsr) throws Exception {

    init();

    PolicyRequestsInfo policyRequestsInfo = resContext.getPolicyRequests();
    List<PolicyRequestInfo> policyRequests = policyRequestsInfo.getPolicyRequests();

    for(PolicyRequestInfo policyRequest : policyRequests) {

      String queueName = policyRequest.getQueueName();
      ClusterWeights clusterWeights = policyRequest.getClusterWeights();
      ArrayList<ClusterWeight> clusterWeightList = clusterWeights.getClusterWeight();

      LOG.info("Updating policy for queue {} to: {}",
          queueName, clusterWeightList);

      Map<SubClusterIdInfo, Float> routerWeights = new HashMap<>();
      Map<SubClusterIdInfo, Float> amRMWeights;

      for(int i=0;i<clusterWeightList.size();i++){
        routerWeights.put(new SubClusterIdInfo(
            clusterWeightList.get(i).getCluster()),
            clusterWeightList.get(i).getWeight());
      }
      amRMWeights = routerWeights;

      WeightedLocalityPolicyManager manager =
          new WeightedLocalityPolicyManager();
      manager.setQueue(queueName);
      manager.getWeightedPolicyInfo().setRouterPolicyWeights(routerWeights);
      manager.getWeightedPolicyInfo().setAMRMPolicyWeights(amRMWeights);

      SubClusterPolicyConfiguration spc = null;
      // serializeConf it in a context
      try {
        spc = manager.serializeConf();
      } catch (FederationPolicyInitializationException e) {
        LOG.error("serializeConf error",e);
      }

      federationFacade.setPolicyConfiguration(spc);
    }

    return Response.status(Response.Status.OK).entity(
        new PolicyUpdateResponseInfo("Update policy success!")).build();
  }


  @POST
  @Path(GPGWSConsts.APP_HOME_IMPORT)
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public Response importAppHomeByClusterId(
      @QueryParam(GPGWSConsts.CLUSTER_ID) String clusterId,
      @Context HttpServletRequest hsr) throws Exception {

    init();

    LOG.info("ImportAppHome request clusterId: " + clusterId);

    if(StringUtils.isNotBlank(clusterId)){
      SubClusterId subClusterId = SubClusterId.newInstance(clusterId);

      SubClusterInfo subClusterInfo =
          federationFacade.getSubCluster(subClusterId);
      LOG.info("ImportAppHome subClusterInfo: " + subClusterInfo.toString());

      String rmWebServiceAddress = getHttpSchemePrefix(conf) +
          subClusterInfo.getRMWebServiceAddress().trim();
      LOG.info("Contacting RM at: " + rmWebServiceAddress);

      AppsInfo appsInfo = invokeRMWebService(rmWebServiceAddress,
          RMWSConsts.APPS, AppsInfo.class,
          DeSelectFields.DeSelectType.RESOURCE_REQUESTS.toString());

      for (AppInfo appInfo : appsInfo.getApps()) {
        ApplicationId appId = ApplicationId.fromString(appInfo.getAppId());
        federationFacade.addApplicationHomeSubCluster(
            ApplicationHomeSubCluster.newInstance(appId, subClusterId));
        LOG.debug("Added  " + appId + " -> " + subClusterId + " appHomeMapping");
      }

      String succeedMsg = "Success to import " + appsInfo.getApps().size() +
          " appHomeMappings for clusterID: [ " + subClusterId + " ] !";
      LOG.info(succeedMsg);
      return Response.status(Response.Status.OK).entity(succeedMsg).build();

    }else{
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).
          entity("Request clusterId is Empty!").build();
    }

  }
}
