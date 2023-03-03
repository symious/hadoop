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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Default SchedulingNodeTypeSettingPolicy:
 * - Load nodePolicyConfigs (label1,0.5;label,0.2),
 *   update label nodes by global nodes percentage
 *   heartbeat nodes percentage = 1.0 - global nodes percentage
 */
public class PercentageSchedulingNodeTypeSettingPolicy
    implements SchedulingNodeTypeSettingPolicy {

  private static final Logger LOG =
      LoggerFactory.getLogger(PercentageSchedulingNodeTypeSettingPolicy.class);

  private final String nodePolicyConfigs;

  public String getNodePolicyConfigs() {
    return nodePolicyConfigs;
  }

  public PercentageSchedulingNodeTypeSettingPolicy(String nodePolicyConfigs){
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
        List<Double> rateList = new ArrayList<>();
        String[] labelsConfigDetails =
            nodeLabelsSchedulerTypeDetails.split(";");
        for (String labelConfigDetails : labelsConfigDetails) {
          labelList.add(labelConfigDetails.split(",")[0]);
          rateList.add(Double.valueOf(labelConfigDetails.split(",")[1]));
        }
        if (labelList.size() != rateList.size()) {
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
   * label_1,0.5;label2_,0.2;
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
            double globalSchedulerNodesRate =
                Double.parseDouble(globalSchedulerNodesConfigPair[1].trim());

            List<FiCaSchedulerNode> labelNodes = ((CapacityScheduler) rmContext
                .getScheduler()).getNodeTracker()
                .getNodesPerPartition(labelName);
            int labelAllNodeSize = labelNodes.size();

            int globalSchedulerNodeSize =
                (int) Math.round(labelAllNodeSize * globalSchedulerNodesRate);

            if (globalSchedulerNodeSize > labelAllNodeSize) {
              globalSchedulerNodeSize = labelAllNodeSize;
            }

            for (int i = 0; i < globalSchedulerNodeSize; i++) {
              RMNode rmNode = labelNodes.get(i).getRMNode();
              rmNode.setNodeSchedulerType(SchedulingNodeType.GLOBAL);
              LOG.info(
                  "updateAllConfigureLabels set node: " + rmNode.getHostName() +
                      " to to global scheduler");
            }
            for (int i = globalSchedulerNodeSize; i < labelAllNodeSize; i++) {
              RMNode rmNode = labelNodes.get(i).getRMNode();
              rmNode.setNodeSchedulerType(SchedulingNodeType.HEARTBEAT);
              LOG.info(
                  "updateAllConfigureLabels set node: " + rmNode.getHostName() +
                      " to to heartbeat scheduler");
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
            double globalSchedulerNodesRate =
                Double.parseDouble(globalSchedulerNodesConfigPair[1].trim());
            if (labelName.equals(label)) {
              List<FiCaSchedulerNode> labelNodes =
                  ((CapacityScheduler) rmContext
                      .getScheduler()).getNodeTracker()
                      .getNodesPerPartition(labelName);
              int labelAllNodeSize = labelNodes.size();

              int globalSchedulerNodeSize =
                  (int) Math.round(labelAllNodeSize * globalSchedulerNodesRate);

              if (globalSchedulerNodeSize > labelAllNodeSize) {
                globalSchedulerNodeSize = labelAllNodeSize;
              }

              for (int i = 0; i < globalSchedulerNodeSize; i++) {
                RMNode rmNode = labelNodes.get(i).getRMNode();
                rmNode.setNodeSchedulerType(SchedulingNodeType.GLOBAL);
                LOG.info("updateByLabel set node: " + rmNode.getHostName() +
                    " to to global scheduler");
              }
              for (int i = globalSchedulerNodeSize; i < labelAllNodeSize; i++) {
                RMNode rmNode = labelNodes.get(i).getRMNode();
                rmNode.setNodeSchedulerType(SchedulingNodeType.HEARTBEAT);
                LOG.info("updateByLabel set node: " + rmNode.getHostName() +
                    " to to heartbeat scheduler");
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
            if (nodeLabel.equals(labelName)) {
              double globalSchedulerNodesRate =
                  Double.parseDouble(globalSchedulerNodesConfigPair[1].trim());

              List<FiCaSchedulerNode> labelNodes =
                  ((CapacityScheduler) rmContext
                      .getScheduler()).getNodeTracker()
                      .getNodesPerPartition(labelName);
              int labelAllNodeSize = labelNodes.size();

              int expectGlobalSchedulerNodeSize =
                  (int) Math.round(labelAllNodeSize * globalSchedulerNodesRate);

              if (expectGlobalSchedulerNodeSize > labelAllNodeSize) {
                expectGlobalSchedulerNodeSize = labelAllNodeSize;
              }

              int actualGlobalSchedulerNodeSize = 0;

              for (FiCaSchedulerNode labelNode : labelNodes) {
                if (labelNode.getRMNode().getNodeSchedulerType()
                    .equals(SchedulingNodeType.GLOBAL)) {
                  actualGlobalSchedulerNodeSize++;
                }
              }
              LOG.info("actualGlobalSchedulerNodeSize: " +
                  actualGlobalSchedulerNodeSize);
              if (actualGlobalSchedulerNodeSize >
                  expectGlobalSchedulerNodeSize) {
                node.setNodeSchedulerType(SchedulingNodeType.HEARTBEAT);
                LOG.info("actualGlobalSchedulerNodeSize: " +
                    actualGlobalSchedulerNodeSize + " bigger than " +
                    "expectGlobalSchedulerNodeSize: " +
                    expectGlobalSchedulerNodeSize + " ,update node: " +
                    node.getHostName() + " to heartbeat scheduler");
              } else if (actualGlobalSchedulerNodeSize <
                  expectGlobalSchedulerNodeSize) {
                node.setNodeSchedulerType(SchedulingNodeType.GLOBAL);
                LOG.info("actualGlobalSchedulerNodeSize: " +
                    actualGlobalSchedulerNodeSize + " smaller than " +
                    "expectGlobalSchedulerNodeSize: " +
                    expectGlobalSchedulerNodeSize + " ,update node: " +
                    node.getHostName() + " to global scheduler");
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
