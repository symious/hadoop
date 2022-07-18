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

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;

import static org.apache.hadoop.yarn.server.globalpolicygenerator.GPGUtils.*;

import org.apache.hadoop.yarn.server.globalpolicygenerator.GPGMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;

/**
 * TimeLineApplicationCleaner that cleans up old applications from table
 * applicationsHomeSubCluster in FederationStateStore by querying
 * timelineReaderService api.
 */
public class TimeLineApplicationCleaner extends ApplicationCleaner {
  private static final Logger LOG =
      LoggerFactory.getLogger(TimeLineApplicationCleaner.class);

  public static final String APPLICATION_STATE = "YARN_APPLICATION_STATE";

  public static final String APPLICATION_FINISHED_TIMESTAMP =
      "YARN_APPLICATION_FINISHED";

  private GPGMetrics gpgMetrics = GPGMetrics.getMetrics();;

  @Override
  public void run() {
    Date now = new Date();
    LOG.info("TimeLineApplicationCleaner run at time {}", now);

    FederationStateStoreFacade facade = getGPGContext().getStateStoreFacade();

    try {

      // Get all apps from StateStore
      Set<ApplicationId> allStateStoreApps = new HashSet<>();

      // Clean up StateStore entries
      Set<ApplicationId> toDelete = new HashSet<>();

      List<ApplicationHomeSubCluster> applicationHomeSubClusterList =
          facade.getApplicationsHomeSubCluster();
      LOG.debug("applicationHomeSubClusterList: " + applicationHomeSubClusterList);
      gpgMetrics.incrSumAppStateStores(applicationHomeSubClusterList.size());

      Client httpClient = null;
      ClientResponse resp = null;

      for (ApplicationHomeSubCluster app : applicationHomeSubClusterList) {
        try{
          ApplicationId applicationId = app.getApplicationId();
          SubClusterId homeSubCluster = app.getHomeSubCluster();
          allStateStoreApps.add(applicationId);

          String queryTimelineAddress = getQueryTimeLineAddress();
          LOG.debug("getQueryTimeLineAddress: " + queryTimelineAddress);

          String queryUrl = queryTimelineAddress + "/ws/v2/timeline/" +
              "clusters/" + homeSubCluster.getId() + "/apps/" +
              applicationId.toString() + "?fields=ALL";
          URI uri = URI.create(queryUrl);
          LOG.debug("query timeline Url: " + queryUrl);

          httpClient = createClient();
          resp = getResponse(httpClient, uri);
          TimelineEntity entity = resp.getEntity(TimelineEntity.class);

          //get app state
          String applicationState =
              (String) entity.getInfo().get(APPLICATION_STATE);

          //get app finish timestamp
          long appFinishedStamp = Long.MAX_VALUE;
          NavigableSet<TimelineEvent> timelineEvents = entity.getEvents();
          for (TimelineEvent event : timelineEvents) {
            if (event.getId().equals(APPLICATION_FINISHED_TIMESTAMP)) {
              appFinishedStamp = event.getTimestamp();
              LOG.debug(
                  "ApplicationID: " + applicationId + " ,appFinishedStamp: " +
                      appFinishedStamp);
            }
          }

          //add finished app to deleteListls
          if((applicationState.equals(YarnApplicationState.FINISHED.toString()) ||
              applicationState.equals(YarnApplicationState.FAILED.toString()) ||
              applicationState.equals(YarnApplicationState.KILLED.toString())) &&
              (System.currentTimeMillis() - appFinishedStamp) >
                  getAppHomeExpireMinTime()){
            if(LOG.isDebugEnabled()){
              LOG.debug("ApplicationID: " + applicationId + " finished " +
                  (System.currentTimeMillis() - appFinishedStamp) +
                  " ms, exceeds ExpireMinTime : " + getAppHomeExpireMinTime() +
                  " ms, add it to deleted list!");
            }
            toDelete.add(applicationId);
          }
        }catch (Exception e){
          gpgMetrics.incrFailedQueryAppsFromTimeline();
          LOG.error("Query app: " + app.getApplicationId() + " from timeline " +
              "failed!",e);
        } finally {
          if (resp != null) {
            resp.close();
          }
          httpClient.destroy();
        }
      }

      LOG.info(allStateStoreApps.size() + " app entries in " +
          "FederationStateStore, deleting " + toDelete.size() +
          " applications from stateStore");
      LOG.debug("Apps to delete: {}", toDelete);

      int i = 0;
      for (ApplicationId appId : toDelete) {
        try {
          LOG.debug("Deleting " + appId + " from stateStore");
          facade.deleteApplicationHomeSubCluster(appId);
          i++;
        } catch (Exception e) {
          LOG.error("deleteApplicationHomeSubCluster failed at application " +
                  appId, e);
        }
      }
      gpgMetrics.incrDeletedAppStateStores(i);

    } catch (Exception e) {
      LOG.error("Application cleaner started at time " + now + " fails: ", e);
    }

  }

}
