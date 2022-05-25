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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Util class for Router Yarn client API calls.
 */
public final class RouterYarnClientUtils {

  private static final Logger LOG =
      LoggerFactory.getLogger(RouterYarnClientUtils.class);

  private RouterYarnClientUtils() {

  }

  public static GetClusterMetricsResponse merge(
      Collection<GetClusterMetricsResponse> responses) {
    YarnClusterMetrics tmp = YarnClusterMetrics.newInstance(0);
    for (GetClusterMetricsResponse response : responses) {
      YarnClusterMetrics metrics = response.getClusterMetrics();
      tmp.setNumNodeManagers(
          tmp.getNumNodeManagers() + metrics.getNumNodeManagers());
      tmp.setNumActiveNodeManagers(
          tmp.getNumActiveNodeManagers() + metrics.getNumActiveNodeManagers());
      tmp.setNumDecommissionedNodeManagers(
          tmp.getNumDecommissionedNodeManagers() + metrics
              .getNumDecommissionedNodeManagers());
      tmp.setNumLostNodeManagers(
          tmp.getNumLostNodeManagers() + metrics.getNumLostNodeManagers());
      tmp.setNumRebootedNodeManagers(tmp.getNumRebootedNodeManagers() + metrics
          .getNumRebootedNodeManagers());
      tmp.setNumUnhealthyNodeManagers(
          tmp.getNumUnhealthyNodeManagers() + metrics
              .getNumUnhealthyNodeManagers());
    }
    return GetClusterMetricsResponse.newInstance(tmp);
  }

  public static List<ApplicationReport> mergeApps(
      Collection<GetApplicationsResponse> responses) {
    List<ApplicationReport> sumApplicationReportList = new ArrayList<>();
    for (GetApplicationsResponse response : responses) {
      List<ApplicationReport> applicationReportList =
          response.getApplicationList();
      sumApplicationReportList.addAll(applicationReportList);
    }
    LOG.info("GetApplicationsResponse size: " + sumApplicationReportList.size());
    return sumApplicationReportList;
  }

  public static GetClusterNodesResponse mergeNodes(
      Collection<GetClusterNodesResponse> responses) {
    List<NodeReport> sumNodeReportList = new ArrayList<>();
    for (GetClusterNodesResponse response : responses) {
      List<NodeReport> nodeReportList =
          response.getNodeReports();
      sumNodeReportList.addAll(nodeReportList);
    }
    LOG.info("GetClusterNodesResponse size: " + sumNodeReportList.size());
    return GetClusterNodesResponse.newInstance(sumNodeReportList);
  }

}
