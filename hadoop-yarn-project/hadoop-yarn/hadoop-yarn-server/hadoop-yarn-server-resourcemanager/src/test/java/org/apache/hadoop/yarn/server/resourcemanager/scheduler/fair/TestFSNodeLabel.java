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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


@SuppressWarnings("unchecked")
public class TestFSNodeLabel extends FairSchedulerTestBase {
  private final static String ALLOC_FILE =
      new File(TEST_DIR, "test-queues").getAbsolutePath();

  @Before
  public void setUp() throws IOException {
    scheduler = new FairScheduler();
    conf = createConfiguration();
    conf.setBoolean(YarnConfiguration.NODE_LABELS_ENABLED, true);
    resourceManager = new ResourceManager();
    resourceManager.init(conf);

    RMContext context = resourceManager.getRMContext();
    // TODO: This test should really be using MockRM. For now starting stuff
    // that is needed at a bare minimum.
    ((AsyncDispatcher)context.getDispatcher()).start();
    context.getStateStore().start();

    // to initialize the master key
    context.getContainerTokenSecretManager().rollMasterKey();

    scheduler.setRMContext(context);
  }

  @After
  public void tearDown() {
    if (scheduler != null) {
      scheduler.stop();
      scheduler = null;
    }
    if (resourceManager != null) {
      resourceManager.stop();
      resourceManager = null;
    }
    QueueMetrics.clearQueueMetrics();
    DefaultMetricsSystem.shutdown();
  }

  @Test
  public void testAssignmentWithNodeLabel() throws Exception {
    conf.set(FairSchedulerConfiguration.ALLOCATION_FILE, ALLOC_FILE);

    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<queue name=\"root\">");
    out.println("<queue name=\"queueA\">");
    out.println("<minResources>1024mb,0vcores</minResources>");
    out.println("</queue>");
    out.println("<queue name=\"queueB\">");
    out.println("<minResources>1024mb,0vcores</minResources>");
    out.println("<accessibleNodeLabel>label2</accessibleNodeLabel>");
    out.println("</queue>");
    out.println("<queue name=\"queueC\">");
    out.println("<accessibleNodeLabel>label3,label4</accessibleNodeLabel>");
    out.println("</queue>");
    out.println("</queue>");
    out.println("</allocations>");
    out.close();

    scheduler.init(conf);
    scheduler.start();
    scheduler.reinitialize(conf, resourceManager.getRMContext());

    // Create 3 nodes
    RMNode node1 =
        MockNodes.newNodeInfo(1, Resources.createResource(4 * 1024, 4), 1,
            "127.0.0.1");
    NodeAddedSchedulerEvent nodeEvent1 = new NodeAddedSchedulerEvent(node1);
    scheduler.handle(nodeEvent1);
    RMNode node2 =
        MockNodes.newNodeInfo(1, Resources.createResource(4 * 1024, 4), 1,
            "127.0.0.2");
    NodeAddedSchedulerEvent nodeEvent2 = new NodeAddedSchedulerEvent(node2);
    scheduler.handle(nodeEvent2);
    RMNode node3 =
        MockNodes.newNodeInfo(1, Resources.createResource(4 * 1024, 4), 1,
            "127.0.0.3");
    NodeAddedSchedulerEvent nodeEvent3 = new NodeAddedSchedulerEvent(node3);
    scheduler.handle(nodeEvent3);
    // add node label
    Set<NodeLabel> clusterLabel = new HashSet<>();
    clusterLabel.add(NodeLabel.newInstance("label1"));
    clusterLabel.add(NodeLabel.newInstance("label2"));
    clusterLabel.add(NodeLabel.newInstance("label3"));
    scheduler.getLabelsManager().addToCluserNodeLabels(clusterLabel);
    Map<NodeId, Set<String>> nodeLabelMap = new HashMap<>();
    // node1: label1
    // node2: label2, label3
    // node3:
    Set<String> nodeLabel1 = new HashSet<>();
    nodeLabel1.add(RMNodeLabelsManager.NO_LABEL);
    Set<String> nodeLabel2 = new HashSet<>();
    nodeLabel2.add("label2");
    Set<String> nodeLabel3 = new HashSet<>();
    nodeLabel3.add("label3");
    nodeLabelMap.put(node1.getNodeID(), nodeLabel1);
    nodeLabelMap.put(node2.getNodeID(), nodeLabel2);
    nodeLabelMap.put(node3.getNodeID(), nodeLabel3);
    scheduler.getLabelsManager().addLabelsToNode(nodeLabelMap);

    //case1 : app submitted into queue without node label could allocated
    // on each node
    ApplicationAttemptId appAttId1 = createSchedulingRequest(3074, 1,
        "root.queueA", "user1", 3, 3);
    NodeUpdateSchedulerEvent nodeUpdate11 = new NodeUpdateSchedulerEvent(node1);
    NodeUpdateSchedulerEvent nodeUpdate12 = new NodeUpdateSchedulerEvent(node2);
    NodeUpdateSchedulerEvent nodeUpdate13 = new NodeUpdateSchedulerEvent(node3);

    scheduler.update();
    scheduler.handle(nodeUpdate11);
    scheduler.handle(nodeUpdate12);
    scheduler.handle(nodeUpdate13);

    // app2 will allocated on node1
    for (RMContainer container :
        scheduler.getSchedulerApp(appAttId1).getLiveContainers()) {
      assertTrue("Request with no label did not run on a host with no label",
          container.getAllocatedNode().equals(node1.getNodeID()));
    }

    AppAttemptRemovedSchedulerEvent appRemovedEvent1 =
        new AppAttemptRemovedSchedulerEvent(
        appAttId1, RMAppAttemptState.FINISHED, false);

    scheduler.handle(appRemovedEvent1);
    scheduler.update();

    //case2 : app submitted into queueB could only be allocated on node1
    // now super app finished
    ApplicationAttemptId appAttId2 = createSchedulingRequest(1024, 1,
        "root.queueB", "user1", 3, 3, "label2");
    NodeUpdateSchedulerEvent nodeUpdate21 = new NodeUpdateSchedulerEvent(node1);
    NodeUpdateSchedulerEvent nodeUpdate22 = new NodeUpdateSchedulerEvent(node2);
    NodeUpdateSchedulerEvent nodeUpdate23 = new NodeUpdateSchedulerEvent(node3);

    scheduler.update();
    scheduler.handle(nodeUpdate21);
    scheduler.handle(nodeUpdate22);
    scheduler.handle(nodeUpdate23);

    // app2 will allocated on node2
    for (RMContainer container :
        scheduler.getSchedulerApp(appAttId2).getLiveContainers()) {
      assertTrue("Request for label2 did not run on a host with label2",
          container.getAllocatedNode().equals(node2.getNodeID()));
    }

    AppAttemptRemovedSchedulerEvent appRemovedEvent2 =
        new AppAttemptRemovedSchedulerEvent(
        appAttId2, RMAppAttemptState.FINISHED, false);

    scheduler.handle(appRemovedEvent2);
    scheduler.update();

    //case3 : app submitted into queueC could be allocated on node3
    // now super app finished
    ApplicationAttemptId appAttId3 = createSchedulingRequest(1024, 1,
        "root.queueC", "user1", 3, 3, "label3");
    NodeUpdateSchedulerEvent nodeUpdate31 = new NodeUpdateSchedulerEvent(node1);
    NodeUpdateSchedulerEvent nodeUpdate32 = new NodeUpdateSchedulerEvent(node2);
    NodeUpdateSchedulerEvent nodeUpdate33 = new NodeUpdateSchedulerEvent(node3);

    scheduler.update();
    scheduler.handle(nodeUpdate31);
    scheduler.handle(nodeUpdate32);
    scheduler.handle(nodeUpdate33);

    // app3 will allocated on node2
    for (RMContainer container :
        scheduler.getSchedulerApp(appAttId3).getLiveContainers()) {
      assertTrue("Request for label3 did not run on a host with label3",
          container.getAllocatedNode().equals(node3.getNodeID()));
    }
    AppAttemptRemovedSchedulerEvent appRemovedEvent3 =
        new AppAttemptRemovedSchedulerEvent(
        appAttId3, RMAppAttemptState.FINISHED, false);

    scheduler.handle(appRemovedEvent3);
    scheduler.update();
  }
}
