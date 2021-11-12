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

package org.apache.hadoop.yarn.server.router.fairness;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.router.RouterServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.ROUTER_FAIR_HANDLER_COUNT_KEY_PREFIX;
import static org.apache.hadoop.yarn.server.router.fairness.RouterRpcFairnessConstants.CONCURRENT_SUBCLUSTER_ID;

/**
 * Static fairness policy extending @AbstractRouterRpcFairnessPolicyController
 * and fetching handlers from configuration for all available subClusters.
 * The handlers count will not change for this controller.
 */
public class StaticRouterRpcFairnessPolicyController extends
    AbstractRouterRpcFairnessPolicyController {

  private static final Logger LOG =
      LoggerFactory.getLogger(StaticRouterRpcFairnessPolicyController.class);

  public StaticRouterRpcFairnessPolicyController(Configuration conf)
      throws YarnException {
    init(conf);
  }

  public void init(Configuration conf)
      throws IllegalArgumentException, YarnException {
    super.init(conf);
    // Total handlers configured to process all incoming Rpc.
    int handlerCount = conf.getInt(
        YarnConfiguration.ROUTER_CLIENT_THREAD_COUNT,
        YarnConfiguration.DEFAULT_ROUTER_CLIENT_THREAD_COUNT);

    LOG.info("Handlers available for fairness assignment {} ", handlerCount);

    // Get all subClusters
    Set<String> allClusters = new HashSet<>();
    try {
      Set<SubClusterId> subClusterIds =
          FederationStateStoreFacade.getInstance().
              getSubClusters(false).keySet();
      for(SubClusterId id : subClusterIds){
        allClusters.add(id.getId());
      }
    } catch (Exception e) {
      RouterServerUtil.logAndThrowException("Fail to get all subClusters from "
          + "stateStore ", e);
    }

    // Set to hold subClusters that are not configured with dedicated handlers.
    Set<String> unassignedSubCluster = new HashSet<>();

    // Insert the concurrent subCluster into the set to process together
    allClusters.add(CONCURRENT_SUBCLUSTER_ID);

    for (String subClusterId : allClusters) {
      int dedicatedHandlers =
          conf.getInt(ROUTER_FAIR_HANDLER_COUNT_KEY_PREFIX + subClusterId, 0);
      LOG.info("Dedicated handlers {} for ns {} ", dedicatedHandlers, subClusterId);
      if (dedicatedHandlers > 0) {
        handlerCount -= dedicatedHandlers;
        // Total handlers should not be less than sum of dedicated
        // handlers.
        validateCount(subClusterId, handlerCount, 0);
        insertSubClusterWithPermits(subClusterId, dedicatedHandlers);
        logAssignment(subClusterId, dedicatedHandlers);
      } else {
        unassignedSubCluster.add(subClusterId);
      }
    }

    // Assign remaining handlers equally to remaining subClusters and
    // general pool if applicable.
    if (!unassignedSubCluster.isEmpty()) {
      LOG.info("Unassigned subCluster {}", unassignedSubCluster.toString());
      int handlersPerSubCluster = handlerCount / unassignedSubCluster.size();
      LOG.info("Handlers available per subCluster {}", handlersPerSubCluster);
      for (String subClusterId : unassignedSubCluster) {
        // Each NS should have at least one handler assigned.
        validateCount(subClusterId, handlersPerSubCluster, 1);
        insertSubClusterWithPermits(subClusterId, handlersPerSubCluster);
        logAssignment(subClusterId, handlersPerSubCluster);
      }
    }

    // Assign remaining handlers if any to fan out calls.
    int leftOverHandlers = handlerCount % unassignedSubCluster.size();
    int existingPermits = getAvailablePermits(CONCURRENT_SUBCLUSTER_ID);
    if (leftOverHandlers > 0) {
      LOG.info("Assigned extra {} handlers to commons pool", leftOverHandlers);
      insertSubClusterWithPermits(CONCURRENT_SUBCLUSTER_ID,
          existingPermits + leftOverHandlers);
    }
    LOG.info("Final permit allocation for concurrent ns: {}",
        getAvailablePermits(CONCURRENT_SUBCLUSTER_ID));
  }

  private static void logAssignment(String subClusterId, int count) {
    LOG.info("Assigned {} handlers to subClusterId {} ", count, subClusterId);
  }

  private static void validateCount(String subClusterId, int handlers, int min)
      throws IllegalArgumentException {
    if (handlers < min) {
      String msg =
          "Available handlers " + handlers +
          " lower than min " + min +
          " for subClusterId " + subClusterId;
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }
  }

}
