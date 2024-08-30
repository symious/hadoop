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

package org.apache.hadoop.yarn.server.globalpolicygenerator.applicationcleaner;

import org.apache.commons.collections.MapUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.curator.ZKCuratorManager;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.server.federation.failover.FederationProxyProviderUtil;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.globalpolicygenerator.GPGMetrics;
import org.apache.hadoop.yarn.util.Records;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SyncRMApplicationCleaner that cleans up old applications from table
 * applicationsHomeSubCluster in FederationStateStore by querying
 * RM api.
 */
public class SyncRMApplicationCleaner extends ApplicationCleaner {
  private static final Logger LOG =
      LoggerFactory.getLogger(SyncRMApplicationCleaner.class);

  private final GPGMetrics gpgMetrics = GPGMetrics.getMetrics();
  private final Map<SubClusterId, ApplicationClientProtocol> clientRMProxies =
      new ConcurrentHashMap<>();

  @Override
  public void run() {
    Date now = new Date();
    LOG.info("SyncRMApplicationCleaner run at time {}", now);
    FederationStateStoreFacade facade = getGPGContext().getStateStoreFacade();
    Configuration conf = facade.getConf();
    String baseZNode = conf.get(
        YarnConfiguration.FEDERATION_STATESTORE_ZK_PARENT_PATH,
        YarnConfiguration.DEFAULT_FEDERATION_STATESTORE_ZK_PARENT_PATH);
    LOG.info("baseZNode: " + baseZNode);

    ZKCuratorManager zk = null;
    CuratorFramework curator;

    try {
      zk = new ZKCuratorManager(conf);
      zk.start();
      curator = zk.getCurator();

      List<ApplicationHomeSubCluster> applicationHomeSubClusterList =
          facade.getApplicationsHomeSubCluster();
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "applicationHomeSubClusterList: " + applicationHomeSubClusterList);
      }
      gpgMetrics.incrSumAppStateStores(applicationHomeSubClusterList.size());

      int deletedAppStateStores = 0;

      for (ApplicationHomeSubCluster app : applicationHomeSubClusterList) {
        ApplicationId applicationId = app.getApplicationId();
        SubClusterId homeSubCluster = app.getHomeSubCluster();

        Map<SubClusterId, SubClusterInfo> activeSubClusters =
            facade.getSubClusters(true);
        if (MapUtils.isEmpty(activeSubClusters) ||
            !activeSubClusters.containsKey(homeSubCluster)) {
          LOG.warn("applicationId: " + applicationId + " ,homeSubCluster: " +
              homeSubCluster + " is not active, skip it first!");
          continue;
        }

        try {
          ApplicationClientProtocol clientRMProxy =
              getClientRMProxyForSubCluster(facade.getConf(), homeSubCluster);

          GetApplicationReportRequest request = Records
              .newRecord(GetApplicationReportRequest.class);
          request.setApplicationId(applicationId);
          GetApplicationReportResponse getApplicationReportResponse =
              clientRMProxy.getApplicationReport(request);
          YarnApplicationState applicationState =
              getApplicationReportResponse.getApplicationReport()
                  .getYarnApplicationState();

          if (LOG.isDebugEnabled()) {
            LOG.debug("homeSubCluster: " + homeSubCluster + " ,appId: " +
                applicationId + " ,applicationState: " +
                applicationState);
          }

          //delete finished app
          if ((applicationState.equals(YarnApplicationState.FINISHED) ||
              applicationState.equals(YarnApplicationState.FAILED) ||
              applicationState.equals(YarnApplicationState.KILLED))) {
            facade.deleteApplicationHomeSubCluster(applicationId);
            deletedAppStateStores++;
            LOG.info("Deleted " + applicationId +
                " from stateStore, due to application state: " +
                applicationState);
          }

        } catch (ApplicationNotFoundException e) {
          //check zk state time
          String appZNode =
              baseZNode + "/applications/" + applicationId.toString();
          long creationTime = getZNodeCreateTime(curator, appZNode);
          if (LOG.isDebugEnabled()) {
            LOG.debug("homeSubCluster: " + homeSubCluster + " ,appId: " +
                applicationId + " ,creationTime: " + creationTime);
          }
          if ((creationTime > 0) &&
              ((Time.now() - creationTime) > getAppHomeExpireMinTime())) {
            facade.deleteApplicationHomeSubCluster(applicationId);
            deletedAppStateStores++;
            LOG.info("Deleted " + applicationId +
                " from stateStore, due to timeout to found from RM!");
          }
        } catch (Exception e) {
          gpgMetrics.incrFailedQueryApps();
          LOG.error("query app: " + app.getApplicationId() +
              " occur unKnow exception, ignore it!", e);
        }
      }

      gpgMetrics.incrDeletedAppStateStores(deletedAppStateStores);

    } catch (Exception e) {
      LOG.error("Application cleaner started at time " + now + " fails: ", e);
    } finally {
      if (zk != null) {
        zk.close();
      }
    }
  }

  private long getZNodeCreateTime(CuratorFramework curator, String zNodePath) {
    long creationTime = 0;
    try {
      Stat stat = curator.checkExists().forPath(zNodePath);
      if (stat != null) {
        creationTime = stat.getCtime();
        LOG.debug("ZNode creation time: " + creationTime);
      } else {
        LOG.error("ZNode does not exist.");
      }
    } catch (Exception e) {
      LOG.error("unKnown exception: ", e);
    }
    return creationTime;
  }

  private ApplicationClientProtocol getClientRMProxyForSubCluster(
      Configuration configuration, SubClusterId subClusterId) {

    if (clientRMProxies.containsKey(subClusterId)) {
      return clientRMProxies.get(subClusterId);
    }

    ApplicationClientProtocol clientRMProxy = null;
    try {
      clientRMProxy = FederationProxyProviderUtil.createRMProxy(configuration,
          ApplicationClientProtocol.class, subClusterId,
          UserGroupInformation.getCurrentUser());
    } catch (Exception e) {
      LOG.error("Unable to create the interface to reach the SubCluster " +
          subClusterId, e);
    }

    clientRMProxies.put(subClusterId, clientRMProxy);
    return clientRMProxy;
  }

}


