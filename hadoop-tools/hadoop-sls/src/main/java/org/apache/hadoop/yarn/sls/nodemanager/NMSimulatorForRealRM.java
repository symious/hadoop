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

package org.apache.hadoop.yarn.sls.nodemanager;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.ServerRMProxy;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.sls.scheduler.ContainerSimulator;
import org.apache.hadoop.yarn.sls.scheduler.TaskRunner;
import org.apache.hadoop.yarn.sls.utils.SLSUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Logger;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

@Private
@Unstable
public class NMSimulatorForRealRM extends TaskRunner.Task {
  // node resource
  private RMNode node;
  // master key
  private MasterKey masterKey;
  // container Key
  private MasterKey containerKey = null;
  // containers with various STATE
  private List<ContainerId> completedContainerList;
  private List<ContainerId> releasedContainerList;
  private DelayQueue<ContainerSimulator> containerQueue;
  private Map<ContainerId, ContainerSimulator> runningContainers;
  private List<ContainerId> amContainerList;

  // heart beat response id
  private int RESPONSE_ID = 0;
  private Configuration conf = null;
  private ResourceTracker resourceTracker = null;
  private UserGroupInformation ugi;

  private final static Logger LOG = Logger.getLogger(NMSimulator.class);

  public void init(String nodeIdStr, int memory, int cores, int dispatchTime,
      int heartBeatInterval, final Configuration conf)
      throws IOException, YarnException, InterruptedException {
    super.init(dispatchTime, dispatchTime + 1000000L * heartBeatInterval,
        heartBeatInterval);
    // create resource
    String rackHostName[] = SLSUtils.getRackHostName(nodeIdStr);
    this.node = NodeInfo.newNodeInfo(rackHostName[0], rackHostName[1],
        BuilderUtils.newResource(memory, cores));
    this.conf = conf;
    // init data structures
    completedContainerList =
        Collections.synchronizedList(new ArrayList<ContainerId>());
    releasedContainerList =
        Collections.synchronizedList(new ArrayList<ContainerId>());
    containerQueue = new DelayQueue<ContainerSimulator>();
    amContainerList =
        Collections.synchronizedList(new ArrayList<ContainerId>());
    runningContainers =
        new ConcurrentHashMap<ContainerId, ContainerSimulator>();

    ugi = UserGroupInformation.createRemoteUser(this.node.toString());
    resourceTracker = ugi.doAs(new PrivilegedExceptionAction<ResourceTracker>() {
      @Override
      public ResourceTracker run() throws Exception {
        return ServerRMProxy.createRMProxy(conf, ResourceTracker.class);
      }
    });

  }

  @Override
  public void firstStep() {
    //register nodemanager
    // register NM with RM
    RegisterNodeManagerRequest req =
        Records.newRecord(RegisterNodeManagerRequest.class);
    req.setNodeId(node.getNodeID());
    req.setResource(node.getTotalCapability());
    req.setHttpPort(80);

    RegisterNodeManagerResponse response = null;
    try {
      response = resourceTracker.registerNodeManager(req);
    } catch (Exception e) {
      LOG.error("NMSimulator register rm failed!",e);
    }
    masterKey = response.getNMTokenMasterKey();
  }

  @Override
  public void middleStep() throws Exception {
    // we check the lifetime for each running containers
    long begin = System.currentTimeMillis();
    long endTime = System.currentTimeMillis();
    ContainerSimulator cs = null;

    synchronized (completedContainerList) {
      while ((cs = containerQueue.poll()) != null) {
        runningContainers.remove(cs.getId());
        completedContainerList.add(cs.getId());
        if (LOG.isDebugEnabled())
          LOG.debug(MessageFormat
              .format("node ({0}) Container {1} has completed",
                  this.node.getNodeID(), cs.getId()));
      }
    }
    endTime = System.currentTimeMillis();

    // send heart beat
    NodeHeartbeatRequest beatRequest =
        Records.newRecord(NodeHeartbeatRequest.class);
    beatRequest.setLastKnownNMTokenMasterKey(masterKey);
    beatRequest.setLastKnownNMTokenMasterKey(containerKey);
    NodeStatus ns = Records.newRecord(NodeStatus.class);

    ns.setContainersStatuses(generateContainerStatusList());
    ns.setNodeId(node.getNodeID());
    ns.setKeepAliveApplications(new ArrayList<ApplicationId>());
    ns.setResponseId(RESPONSE_ID++);
    ns.setNodeHealthStatus(
        NodeHealthStatus.newInstance(true, "", System.currentTimeMillis()));
    beatRequest.setNodeStatus(ns);

    NodeHeartbeatResponse beatResponse =
        resourceTracker.nodeHeartbeat(beatRequest);
    updateMasterKeys(beatResponse);
    endTime = System.currentTimeMillis();

    if (!beatResponse.getContainersToCleanup().isEmpty()) {
      // remove from queue
      synchronized (releasedContainerList) {
        for (ContainerId containerId : beatResponse.getContainersToCleanup()) {
          if (amContainerList.contains(containerId)) {
            // AM container (not killed?, only release)
            synchronized (amContainerList) {
              amContainerList.remove(containerId);
            }
            if (LOG.isDebugEnabled())
              LOG.debug(MessageFormat.format(
                  "NodeManager {0} releases " + "an AM ({1}).",
                  node.getNodeID(), containerId));
          } else {
            cs = runningContainers.remove(containerId);
            containerQueue.remove(cs);
            releasedContainerList.add(containerId);
            if (LOG.isDebugEnabled())
              LOG.debug(MessageFormat.format(
                  "NodeManager {0} releases a " + "container ({1}).",
                  node.getNodeID(), containerId));
          }
        }
      }
    }
    if (LOG.isDebugEnabled()){
      LOG.debug(this.node.getNodeID().toString() + " middleStep cost time(ms): " + (System.currentTimeMillis() - begin));
    }
    if (beatResponse.getNodeAction() == NodeAction.SHUTDOWN) {
      lastStep();
    }
  }

  private void updateMasterKeys(NodeHeartbeatResponse response) {
    // See if the master-key has rolled over
    MasterKey updatedMasterKey = response.getContainerTokenMasterKey();
    if (updatedMasterKey != null) {
      // Will be non-null only on roll-over on RM side
      containerKey = updatedMasterKey;
    }

    updatedMasterKey = response.getNMTokenMasterKey();
    if (updatedMasterKey != null) {
      masterKey = updatedMasterKey;
    }
  }

  @Override
  public void lastStep() {
    if (this.resourceTracker != null) {
      RPC.stopProxy(this.resourceTracker);
    }
  }

  /**
   * catch status of all containers located on current node
   */
  private ArrayList<ContainerStatus> generateContainerStatusList() {
    ArrayList<ContainerStatus> csList = new ArrayList<ContainerStatus>();
    // add running containers
    for (ContainerSimulator container : runningContainers.values()) {
      csList.add(newContainerStatus(container.getId(), ContainerState.RUNNING,
          ContainerExitStatus.SUCCESS));
    }
    synchronized (amContainerList) {
      for (ContainerId cId : amContainerList) {
        csList.add(newContainerStatus(cId, ContainerState.RUNNING,
            ContainerExitStatus.SUCCESS));
      }
    }
    // add complete containers
    synchronized (completedContainerList) {
      for (ContainerId cId : completedContainerList) {
        if (LOG.isDebugEnabled())
          LOG.debug(MessageFormat.format(
              "NodeManager {0} completed" + " container ({1}).",
              node.getNodeID(), cId));
        csList.add(newContainerStatus(cId, ContainerState.COMPLETE,
            ContainerExitStatus.SUCCESS));
      }
      completedContainerList.clear();
    }
    // released containers
    synchronized (releasedContainerList) {
      for (ContainerId cId : releasedContainerList) {
        if (LOG.isDebugEnabled())
          LOG.debug(MessageFormat.format(
              "NodeManager {0} released container" + " ({1}).",
              node.getNodeID(), cId));
        csList.add(newContainerStatus(cId, ContainerState.COMPLETE,
            ContainerExitStatus.ABORTED));
      }
      releasedContainerList.clear();
    }
    return csList;
  }

  private ContainerStatus newContainerStatus(ContainerId cId,
      ContainerState state, int exitState) {
    ContainerStatus cs = Records.newRecord(ContainerStatus.class);
    cs.setContainerId(cId);
    cs.setState(state);
    cs.setExitStatus(exitState);
    return cs;
  }

  public RMNode getNode() {
    return node;
  }

  /**
   * launch a new container with the given life time
   */
  public void addNewContainer(Container container, long lifeTimeMS) {
    if (LOG.isDebugEnabled())
      LOG.debug(MessageFormat.format(
          "NodeManager {0} launches a new " + "container ({1}).",
          node.getNodeID(), container.getId()));
    if (lifeTimeMS != -1) {
      // normal container
      ContainerSimulator cs =
          new ContainerSimulator(container.getId(), container.getResource(),
              lifeTimeMS + System.currentTimeMillis(), lifeTimeMS,
              container.getAllocationRequestId());
      containerQueue.add(cs);
      runningContainers.put(cs.getId(), cs);
    } else {
      // AM container
      // -1 means AMContainer
      synchronized (amContainerList) {
        amContainerList.add(container.getId());
      }
    }
  }

  /**
   * clean up an AM container and add to completed list
   *
   * @param containerId id of the container to be cleaned
   */
  public void cleanupContainer(ContainerId containerId) {
    synchronized (amContainerList) {
      amContainerList.remove(containerId);
    }
    synchronized (completedContainerList) {
      completedContainerList.add(containerId);
    }
  }

  @VisibleForTesting
  Map<ContainerId, ContainerSimulator> getRunningContainers() {
    return runningContainers;
  }

  @VisibleForTesting
  List<ContainerId> getAMContainers() {
    return amContainerList;
  }

  @VisibleForTesting
  List<ContainerId> getCompletedContainers() {
    return completedContainerList;
  }
}
