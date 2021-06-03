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

package org.apache.hadoop.yarn.server.router.service;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.thirdparty.com.google.common.collect.Sets;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.DeSelectFields;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.util.MonotonicClock;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static javax.servlet.http.HttpServletResponse.SC_OK;


/**
 * Implements {@link FederationStateStore} and provides a service for
 * participating in the federation membership.
 */
public class ApplicationCleanerService extends AbstractService {

  public static final Logger LOG =
      LoggerFactory.getLogger(ApplicationCleanerService.class);

  private static final MonotonicClock CLOCK = new MonotonicClock();

  private Configuration config;

  private ScheduledExecutorService scheduledExecutorService;

  private Runnable applicationCleaner = new Runnable() {
    @Override
    public void run() {
      try {
        doApplicationsClean();
      } catch (Exception e) {
        LOG.error("Delete old appHomeCLuster relationship failed!", e);
      }
    }
  };

  private FederationStateStoreFacade federationFacade = null;
  private int minRouterSuccessCount;
  private int maxRouterRetry;
  private long routerQueryIntervalMs;
  private long appCleanerIntervalMs;
  private int maxWarnApps;

  public ApplicationCleanerService() {
    super(ApplicationCleanerService.class.getName());
    LOG.info("ApplicationCleanerService initialized");
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {

    this.config = conf;

    this.federationFacade = FederationStateStoreFacade.getInstance();

    appCleanerIntervalMs = conf.getLong(
            YarnConfiguration.ROUTER_APP_CLEANER_INTERVAL_MS,
            YarnConfiguration.DEFAULT_ROUTER_APP_CLEANER_INTERVAL_MS);

    maxWarnApps = conf.getInt(
            YarnConfiguration.ROUTER_MAX_APPS_WARN_THRESHOLD,
            YarnConfiguration.DEFAULT_ROUTER_MAX_APPS_WARN_THRESHOLD);
    if (maxWarnApps <= 0) {
      maxWarnApps =
              YarnConfiguration.DEFAULT_ROUTER_MAX_APPS_WARN_THRESHOLD;
    }

    String routerSpecString =
        this.config.get(YarnConfiguration.APP_CLEANER_CONTACT_ROUTER_SPEC,
            YarnConfiguration.DEFAULT_APP_CLEANER_CONTACT_ROUTER_SPEC);
    String[] specs = routerSpecString.split(",");
    if (specs.length != 3) {
      throw new YarnException("Expect three comma separated values in "
          + YarnConfiguration.APP_CLEANER_CONTACT_ROUTER_SPEC + " but get "
          + routerSpecString);
    }
    this.minRouterSuccessCount = Integer.parseInt(specs[0]);
    this.maxRouterRetry = Integer.parseInt(specs[1]);
    this.routerQueryIntervalMs = Long.parseLong(specs[2]);

    if (this.minRouterSuccessCount > this.maxRouterRetry) {
      throw new YarnException("minRouterSuccessCount "
          + this.minRouterSuccessCount
          + " should not be larger than maxRouterRetry" + this.maxRouterRetry);
    }
    if (this.minRouterSuccessCount <= 0) {
      throw new YarnException("minRouterSuccessCount "
          + this.minRouterSuccessCount + " should be positive");
    }

    LOG.info(
        "Initialized AppCleaner with Router query with min success {}, "
            + "max retry {}, retry interval {}",
        this.minRouterSuccessCount, this.maxRouterRetry,
        DurationFormatUtils.formatDurationISO(this.routerQueryIntervalMs));

    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    if(appCleanerIntervalMs > 0){
      scheduledExecutorService =
          HadoopExecutors.newSingleThreadScheduledExecutor();
      scheduledExecutorService.scheduleWithFixedDelay(this.applicationCleaner,
          0, appCleanerIntervalMs, TimeUnit.MILLISECONDS);
      LOG.info("Scheduled application cleaner with interval: {}",
          appCleanerIntervalMs);
    }
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    try {
      if (this.scheduledExecutorService != null
          && !this.scheduledExecutorService.isShutdown()) {
        this.scheduledExecutorService.shutdown();
        LOG.info("Shutdown router ScheduledExecutorService");
      }
    } catch (Exception e) {
      LOG.error("Failed to shutdown ScheduledExecutorService", e);
    }
    super.serviceStop();
  }

  private void doApplicationsClean() throws YarnException {

    long startTime = CLOCK.getTime();

    LOG.info("doApplicationsClean start ......");

    // Get the candidate list from StateStore before calling router
    Set<ApplicationId> allStateStoreApps = new HashSet<>();
    List<ApplicationHomeSubCluster> response =
        federationFacade.getApplicationsHomeSubCluster();
    LOG.debug("ApplicationHomeSubCluster response size = " + response.size() +
        " ,result= " + response);
    for (ApplicationHomeSubCluster app : response) {
      allStateStoreApps.add(app.getApplicationId());
    }
    if(allStateStoreApps.size() >= maxWarnApps){
      LOG.warn("{} app entries in FederationStateStore, need to pay attention!!!"
          , allStateStoreApps.size());
    }else{
      LOG.info("{} app entries in FederationStateStore", allStateStoreApps.size());
    }

    // Get the list of known apps from Router
    Set<ApplicationId> routerApps = getRouterKnownApplications();
    LOG.info("{} known applications from Router", routerApps.size());

    // Clean up StateStore entries, which is not exist in all cluster
    Set<ApplicationId> toDelete =
        Sets.difference(allStateStoreApps, routerApps);
    LOG.info("Deleting {} applications from stateStore", toDelete.size());
    LOG.debug("Apps to delete: {}", toDelete);
    for (ApplicationId appId : toDelete) {
      try {
        LOG.debug("Deleting {} from stateStore", appId);
        federationFacade.deleteApplicationHomeSubCluster(appId);
      } catch (Exception e) {
        LOG.error(
            "deleteApplicationHomeSubCluster failed at application " + appId,
            e);
      }
    }

    long endTime = CLOCK.getTime();
    LOG.info("doApplicationsClean end, cost time: " +
        (endTime - startTime) + " ms!");
  }

  /**
   * Query router for applications.
   *
   * @return the set of applications
   * @throws YarnRuntimeException when router call fails
   */
  public Set<ApplicationId> getAppsFromRouter() throws YarnRuntimeException {
    String webAppAddress = WebAppUtils.getRouterWebAppURLWithScheme(config);
    LOG.info(String.format("Contacting router at: %s", webAppAddress));
    AppsInfo appsInfo = invokeRMWebService(webAppAddress,
        RMWSConsts.APPS, AppsInfo.class,
        DeSelectFields.DeSelectType.RESOURCE_REQUESTS.toString());
    Set<ApplicationId> appSet = new HashSet<>();
    for (AppInfo appInfo : appsInfo.getApps()) {
      appSet.add(ApplicationId.fromString(appInfo.getAppId()));
    }
    return appSet;
  }

  /**
   * Get the list of known applications in the cluster from Router.
   *
   * @return the list of known applications
   * @throws YarnException if get app fails
   */
  public Set<ApplicationId> getRouterKnownApplications() throws YarnException {
    int successCount = 0, totalAttemptCount = 0;
    Set<ApplicationId> resultSet = new HashSet<>();
    while (totalAttemptCount < this.maxRouterRetry) {
      try {
        Set<ApplicationId> routerApps = getAppsFromRouter();
        resultSet.addAll(routerApps);
        LOG.info("Attempt {}: {} known apps from Router, {} in total",
            totalAttemptCount, routerApps.size(), resultSet.size());

        successCount++;
        if (successCount >= this.minRouterSuccessCount) {
          return resultSet;
        }

        // Wait for the next attempt
        try {
          Thread.sleep(this.routerQueryIntervalMs);
        } catch (InterruptedException e) {
          LOG.warn("Sleep interrupted after attempt " + totalAttemptCount);
        }
      } catch (Exception e) {
        LOG.warn("Router query attempt " + totalAttemptCount + " failed ", e);
      } finally {
        totalAttemptCount++;
      }
    }
    throw new YarnException("Only " + successCount
        + " success Router queries after " + totalAttemptCount + " retries");
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

}
