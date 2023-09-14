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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

  private Map<String,Double> checkNodeLabelsSchedulerTypeDetails(
      String nodeLabelsSchedulerTypeDetails) {
    Map<String, Double> labelGlobalRateMap = new HashMap<>();
    if (StringUtils.isBlank(nodeLabelsSchedulerTypeDetails)) {
      LOG.error("Invalid nodeLabelsSchedulerTypeDetails: can't be empty!");
      return null;
    } else {
      try {
        String[] labelsConfigDetails =
            nodeLabelsSchedulerTypeDetails.split(";");
        for (String labelConfigDetails : labelsConfigDetails) {
          String[] labelConfigsArray = labelConfigDetails.split(",");
          if (labelConfigsArray.length != 2) {
            LOG.error(
                "Invalid nodeLabelsSchedulerTypeDetails: Incorrect format, " +
                    "incomplete configuration!");
            return null;
          }
          String labelName = labelConfigsArray[0];
          double labelRate = Double.parseDouble(labelConfigsArray[1]);
          labelGlobalRateMap.put(labelName, labelRate);
        }
      } catch (Exception e) {
        LOG.error("Invalid nodeLabelsSchedulerTypeDetails", e);
        return null;
      }
    }
    return labelGlobalRateMap;
  }

  //update nodes scheduler type, return expectGsNodesCount (global scheduler nodes count)
  private void updateLabelNodesSchedulerType(List<FiCaSchedulerNode> labelNodes,
      int expectGsNodesCount) {
    int totalNodesCount = labelNodes.size();

    for (int i = 0; i < totalNodesCount; i++) {
      RMNode rmNode = labelNodes.get(i).getRMNode();
      if (i < expectGsNodesCount) {
        rmNode.setNodeSchedulerType(SchedulingNodeType.GLOBAL);
        LOG.info("update node: " + rmNode.getHostName() + " to " +
            "global scheduler");
      } else {
        rmNode.setNodeSchedulerType(SchedulingNodeType.HEARTBEAT);
        LOG.info("update node: " + rmNode.getHostName() + " to " +
            "heartbeat scheduler");
      }
    }

  }

  /**
   * label_1,0.5;label2_,0.2;
   * split the labelSchedulerTypeDetails by ";"
   **/
  @Override
  public List<String> updateAllConfigureLabels(RMContext rmContext) {
    long startTime = Time.monotonicNowNanos();
    Map<String, Double> labelGlobalRateMap =
        checkNodeLabelsSchedulerTypeDetails(nodePolicyConfigs);

    List<String> updateGlobalLabels = new ArrayList<>();

    if (MapUtils.isNotEmpty(labelGlobalRateMap)) {
      try {
        List<String> allLabels = ((CapacityScheduler) rmContext
            .getScheduler()).getNodeTracker().getPartitions();

        for (String label : allLabels) {
          List<FiCaSchedulerNode> labelAllNodes = ((CapacityScheduler) rmContext
              .getScheduler()).getNodeTracker().getNodesPerPartition(label);

          int totalNodesCount = 0;
          int expectGsNodesCount = 0;

          if (CollectionUtils.isNotEmpty(labelAllNodes)) {
            totalNodesCount = labelAllNodes.size();
            double gsNodesRate = 0;
            if (labelGlobalRateMap.containsKey(label)) {
              gsNodesRate = labelGlobalRateMap.get(label);
            }
            expectGsNodesCount =
                Math.min((int) Math.round(totalNodesCount * gsNodesRate),
                    totalNodesCount);
            updateLabelNodesSchedulerType(labelAllNodes, expectGsNodesCount);
          }

          if (labelGlobalRateMap.containsKey(label)) {
            updateGlobalLabels.add(label);
          }
          LOG.info("updateAllConfigureLabels for label " + label +
              " ,globalSchedulerNodeSize: " + expectGsNodesCount +
              " ,heartbeatSchedulerNodeSize: " +
              (totalNodesCount - expectGsNodesCount));
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
    return updateGlobalLabels;
  }

  @Override
  public void updateByLabel(RMContext rmContext, String label) {
    long startTime = Time.monotonicNowNanos();
    Map<String, Double> labelGlobalRateMap =
        checkNodeLabelsSchedulerTypeDetails(nodePolicyConfigs);

    if (MapUtils.isNotEmpty(labelGlobalRateMap)) {
      try {
        List<FiCaSchedulerNode> labelAllNodes = ((CapacityScheduler) rmContext
            .getScheduler()).getNodeTracker()
            .getNodesPerPartition(label);
        int totalNodesCount = 0;
        int expectGsNodesCount = 0;
        if (CollectionUtils.isNotEmpty(labelAllNodes)) {
          totalNodesCount = labelAllNodes.size();
          double gsNodesRate = 0;
          if (labelGlobalRateMap.containsKey(label)) {
            gsNodesRate = labelGlobalRateMap.get(label);
          }
          expectGsNodesCount =
              Math.min((int) Math.round(totalNodesCount * gsNodesRate),
                  totalNodesCount);
          updateLabelNodesSchedulerType(labelAllNodes, expectGsNodesCount);
        }
        LOG.info("updateByLabel for label " + label +
            " ,globalSchedulerNodeSize: " + expectGsNodesCount +
            " ,heartbeatSchedulerNodeSize: " +
            (totalNodesCount - expectGsNodesCount));

      } catch (Exception e) {
        LOG.error("updateByLabel fail!", e);
      }

    } else {
      LOG.error("updateByLabel fail due to invalid configs!");
    }
    long endTime = Time.monotonicNowNanos();
    LOG.info("updateByLabel: " + label + " cost time: " +
        (endTime - startTime) / 1000 + " us!");
  }


  @Override
  public void updateByNode(RMContext rmContext, RMNode node) {
    long startTime = Time.monotonicNowNanos();
    Map<String, Double> labelGlobalRateMap =
        checkNodeLabelsSchedulerTypeDetails(nodePolicyConfigs);

    String nodeLabel = RMNodeLabelsManager.NO_LABEL;
    Set<String> labels = node.getNodeLabels();
    if (labels.size() > 0) {
      nodeLabel = new ArrayList<>(labels).get(0);
    }

    if (MapUtils.isNotEmpty(labelGlobalRateMap)) {
      try {

        List<FiCaSchedulerNode> labelAllNodes = ((CapacityScheduler) rmContext
            .getScheduler()).getNodeTracker()
            .getNodesPerPartition(nodeLabel);

        int totalNodesCount = 0;
        int expectGsNodesCount = 0;
        int actualGsNodesCount = 0;

        if (CollectionUtils.isNotEmpty(labelAllNodes)) {
          totalNodesCount = labelAllNodes.size();
          if (labelGlobalRateMap.containsKey(nodeLabel)) {
            double gsNodesRate = labelGlobalRateMap.get(nodeLabel);
            expectGsNodesCount =
                Math.min((int) Math.round(totalNodesCount * gsNodesRate),
                    totalNodesCount);
          }
          for (FiCaSchedulerNode labelNode : labelAllNodes) {
            if (labelNode.getRMNode().getNodeSchedulerType()
                .equals(SchedulingNodeType.GLOBAL)) {
              actualGsNodesCount++;
            }
          }
        }
        LOG.info("labelAllNodeSize: " + totalNodesCount +
            " ,expectGlobalSchedulerNodeSize: " + expectGsNodesCount +
            " ,actualGlobalSchedulerNodeSize: " + actualGsNodesCount);

        if (actualGsNodesCount > expectGsNodesCount) {
          node.setNodeSchedulerType(SchedulingNodeType.HEARTBEAT);
          LOG.info("actualGlobalSchedulerNodeSize: " + actualGsNodesCount +
              " bigger than expectGlobalSchedulerNodeSize: " +
              expectGsNodesCount +
              " ,update node: " + node.getHostName() +
              " to heartbeat scheduler");
        } else if (actualGsNodesCount < expectGsNodesCount) {
          node.setNodeSchedulerType(SchedulingNodeType.GLOBAL);
          LOG.info("actualGlobalSchedulerNodeSize: " + actualGsNodesCount +
              " smaller than expectGlobalSchedulerNodeSize: " +
              expectGsNodesCount +
              " ,update node: " + node.getHostName() + " to global scheduler");
        }

      } catch (Exception e) {
        LOG.error("updateByNode fail!", e);
      }

    } else {
      LOG.error("updateByNode fail due to invalid configs!");
    }
    long endTime = Time.monotonicNowNanos();
    LOG.info(
        "updateByNode: " + node.getHostName() + " new nodeSchedulerType: " +
            node.getNodeSchedulerType() + " cost time: " +
            (endTime - startTime) / 1000 + " us!");
  }

}
