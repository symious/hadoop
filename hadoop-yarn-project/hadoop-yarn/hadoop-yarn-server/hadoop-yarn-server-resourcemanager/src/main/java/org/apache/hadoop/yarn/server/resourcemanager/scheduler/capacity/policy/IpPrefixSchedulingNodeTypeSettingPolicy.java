/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.policy;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingNodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Ip Prefix match SchedulingNodeTypeSettingPolicy:
 * - Load nodePolicyConfigs (label1,127.0.0;label2,127.1),
 *   update label nodes to global nodes if ip starts with config prefix
 */
public class IpPrefixSchedulingNodeTypeSettingPolicy
    implements SchedulingNodeTypeSettingPolicy {

  private static final Logger LOG =
      LoggerFactory.getLogger(IpPrefixSchedulingNodeTypeSettingPolicy.class);

  private final String nodePolicyConfigs;

  public String getNodePolicyConfigs() {
    return nodePolicyConfigs;
  }

  public IpPrefixSchedulingNodeTypeSettingPolicy(String nodePolicyConfigs){
    this.nodePolicyConfigs = nodePolicyConfigs;
  }

  private boolean checkNodeLabelsSchedulerTypeDetails(
      String nodeLabelsSchedulerTypeDetails) {
    if (StringUtils.isBlank(nodeLabelsSchedulerTypeDetails)) {
      LOG.error("Invalid nodeLabelsSchedulerTypeDetails: can't be empty!");
      return false;
    } else if (!nodeLabelsSchedulerTypeDetails.contains(",")) {
      LOG.error("Invalid nodeLabelsSchedulerTypeDetails: Incorrect format, " +
          "at least contain one label!");
      return false;
    } else {
      try {
        List<String> labelList = new ArrayList<>();
        List<String> ipPrefixList = new ArrayList<>();
        String[] labelsConfigDetails =
            nodeLabelsSchedulerTypeDetails.split(";");
        for (String labelConfigDetails : labelsConfigDetails) {
          labelList.add(labelConfigDetails.split(",")[0]);
          ipPrefixList.add(labelConfigDetails.split(",")[1]);
        }
        if (labelList.size() != ipPrefixList.size()) {
          LOG.error(
              "Invalid nodeLabelsSchedulerTypeDetails: Incorrect format, " +
                  "incomplete configuration!");
          return false;
        }
      } catch (Exception e) {
        LOG.error("Invalid nodeLabelsSchedulerTypeDetails", e);
        return false;
      }
    }
    return true;
  }

  /**
   * label1,127.0.0;label2,127.1;
   * split the labelSchedulerTypeDetails by ";"
   **/
  @Override
  public List<String> updateAllConfigureLabels(RMContext rmContext) {
    long startTime = Time.monotonicNowNanos();
    boolean legal =
        checkNodeLabelsSchedulerTypeDetails(nodePolicyConfigs);

    List<String> updateLabels = new ArrayList<>();

    if (legal) {
      try {
        for (String labelSchedulerTypeDetails : nodePolicyConfigs
            .split(";")) {
          String labelSchedulerTypeDetailsTrim =
              labelSchedulerTypeDetails.trim();
          if (!labelSchedulerTypeDetailsTrim.isEmpty()) {
            String[] globalSchedulerNodesConfigPair =
                labelSchedulerTypeDetailsTrim.split(",");
            String labelName = globalSchedulerNodesConfigPair[0].trim();
            String ipPrefix = globalSchedulerNodesConfigPair[1].trim();

            List<FiCaSchedulerNode> labelNodes = ((CapacityScheduler) rmContext
                .getScheduler()).getNodeTracker()
                .getNodesPerPartition(labelName);
            int labelAllNodeSize = labelNodes.size();
            int globalSchedulerNodeSize = 0;

            for (FiCaSchedulerNode fiCaSchedulerNode : labelNodes) {
              RMNode rmNode = fiCaSchedulerNode.getRMNode();
              String hostname = rmNode.getHostName();
              String ipAddress =
                  InetAddress.getByName(hostname).getHostAddress();
              if (ipAddress.startsWith(ipPrefix)) {
                rmNode.setNodeSchedulerType(SchedulingNodeType.GLOBAL);
                globalSchedulerNodeSize++;
                LOG.info(
                    "updateAllConfigureLabels set node: " +
                        rmNode.getHostName() +
                        " to to global scheduler");
              } else {
                rmNode.setNodeSchedulerType(SchedulingNodeType.HEARTBEAT);
                LOG.info(
                    "updateAllConfigureLabels set node: " +
                        rmNode.getHostName() +
                        " to to heartbeat scheduler");
              }
            }
            updateLabels.add(labelName);
            LOG.info("updateAllConfigureLabels for label " + labelName +
                " ,globalSchedulerNodeSize: " + globalSchedulerNodeSize +
                " ,heartbeatSchedulerNodeSize: " +
                (labelAllNodeSize - globalSchedulerNodeSize));
          }
        }
      } catch (Exception e) {
        LOG.error("updateAllConfigureLabels fail!", e);
      }

    } else {
      LOG.error("updateAllConfigureLabels fail due to invalid configs!");
    }
    long endTime = Time.monotonicNowNanos();
    LOG.info(
        "updateAllConfigureLabels cost time: " + (endTime - startTime) / 1000 +
            " us!");
    return updateLabels;
  }

  @Override
  public void updateByLabel(RMContext rmContext, String label) {
    long startTime = Time.monotonicNowNanos();
    boolean legal =
        checkNodeLabelsSchedulerTypeDetails(nodePolicyConfigs);

    if (legal) {
      try {
        for (String labelSchedulerTypeDetails : nodePolicyConfigs
            .split(";")) {
          String labelSchedulerTypeDetailsTrim =
              labelSchedulerTypeDetails.trim();
          if (!labelSchedulerTypeDetailsTrim.isEmpty()) {
            String[] globalSchedulerNodesConfigPair =
                labelSchedulerTypeDetailsTrim.split(",");
            String labelName = globalSchedulerNodesConfigPair[0].trim();
            String ipPrefix = globalSchedulerNodesConfigPair[1].trim();
            if (labelName.equals(label)) {
              List<FiCaSchedulerNode> labelNodes =
                  ((CapacityScheduler) rmContext
                      .getScheduler()).getNodeTracker()
                      .getNodesPerPartition(labelName);
              int labelAllNodeSize = labelNodes.size();
              int globalSchedulerNodeSize = 0;

              for (FiCaSchedulerNode fiCaSchedulerNode : labelNodes) {
                RMNode rmNode = fiCaSchedulerNode.getRMNode();
                String hostname = rmNode.getHostName();
                String ipAddress =
                    InetAddress.getByName(hostname).getHostAddress();
                if (ipAddress.startsWith(ipPrefix)) {
                  rmNode.setNodeSchedulerType(SchedulingNodeType.GLOBAL);
                  globalSchedulerNodeSize++;
                  LOG.info(
                      "updateAllConfigureLabels set node: " +
                          rmNode.getHostName() +
                          " to to global scheduler");
                } else {
                  rmNode.setNodeSchedulerType(SchedulingNodeType.HEARTBEAT);
                  LOG.info(
                      "updateAllConfigureLabels set node: " +
                          rmNode.getHostName() +
                          " to to heartbeat scheduler");
                }
              }
              LOG.info("updateByLabel for label " + labelName +
                  " ,globalSchedulerNodeSize: " + globalSchedulerNodeSize +
                  " ,heartbeatSchedulerNodeSize: " +
                  (labelAllNodeSize - globalSchedulerNodeSize));
              break;
            }
          }
        }
      } catch (Exception e) {
        LOG.error("updateByLabel fail!", e);
      }

    } else {
      LOG.error("updateByLabel fail due to invalid configs!");
    }
    long endTime = Time.monotonicNowNanos();
    LOG.info("updateByLabel: " + label + " cost time: " +
        (endTime - startTime) / 1000 +
        " us!");
  }

  @Override
  public void updateByNode(RMContext rmContext, RMNode node) {
    long startTime = Time.monotonicNowNanos();
    boolean legal =
        checkNodeLabelsSchedulerTypeDetails(nodePolicyConfigs);

    String nodeLabel = RMNodeLabelsManager.NO_LABEL;
    Set<String> labels = node.getNodeLabels();
    if (labels.size() > 0) {
      nodeLabel = new ArrayList<>(labels).get(0);
    }

    if (legal) {
      try {
        for (String labelSchedulerTypeDetails : nodePolicyConfigs
            .split(";")) {
          String labelSchedulerTypeDetailsTrim =
              labelSchedulerTypeDetails.trim();

          if (!labelSchedulerTypeDetailsTrim.isEmpty()) {
            String[] globalSchedulerNodesConfigPair =
                labelSchedulerTypeDetailsTrim.split(",");
            String labelName = globalSchedulerNodesConfigPair[0].trim();
            String ipPrefix = globalSchedulerNodesConfigPair[1].trim();
            String hostname = node.getHostName();
            String ipAddress =
                InetAddress.getByName(hostname).getHostAddress();

            if (nodeLabel.equals(labelName)) {
              if (ipAddress.startsWith(ipPrefix)) {
                node.setNodeSchedulerType(SchedulingNodeType.GLOBAL);
                LOG.info("update node: " +
                    node.getHostName() + " to global scheduler");
              } else {
                node.setNodeSchedulerType(SchedulingNodeType.HEARTBEAT);
                LOG.info("update node: " +
                    node.getHostName() + " to heartbeat scheduler");
              }
              break;
            }

          }
        }
      } catch (Exception e) {
        LOG.error("updateByNode fail!", e);
      }

    } else {
      LOG.error("updateByNode fail due to invalid configs!");
    }
    long endTime = Time.monotonicNowNanos();
    LOG.info(
        "updateByNode: " + node.getHostName() + " current nodeSchedulerType: " +
            node.getNodeSchedulerType() + " cost time: " +
            (endTime - startTime) / 1000 + " us!");
  }

}
