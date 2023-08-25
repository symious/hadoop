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

package org.apache.hadoop.yarn.sls.appmaster;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.sls.SLSRunner;
import org.apache.hadoop.yarn.sls.SLSRunnerForRealRM;
import org.apache.hadoop.yarn.sls.scheduler.ContainerSimulator;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Private
@Unstable
public class UnManagedMRAMSimulatorForRealRM extends AMSimulatorForRealRM {
  /*
   * Vocabulary Used: pending -> requests which are NOT yet sent to RM scheduled
   * -> requests which are sent to RM but not yet assigned assigned -> requests
   * which are assigned to a container completed -> request corresponding to
   * which container has completed
   * 
   * Maps are scheduled as soon as their requests are received. Reduces are
   * scheduled when all maps have finished (not support slow-start currently).
   */

  private static final int PRIORITY_REDUCE = 10;
  private static final int PRIORITY_MAP = 20;

  // pending maps
  private LinkedList<ContainerSimulator> pendingMaps =
      new LinkedList<ContainerSimulator>();

  // pending failed maps
  private LinkedList<ContainerSimulator> pendingFailedMaps =
      new LinkedList<ContainerSimulator>();

  // scheduled maps
  private LinkedList<ContainerSimulator> scheduledMaps =
      new LinkedList<ContainerSimulator>();

  // assigned maps
  private Map<ContainerId, ContainerSimulator> assignedMaps =
      new HashMap<ContainerId, ContainerSimulator>();

  // reduces which are not yet scheduled
  private LinkedList<ContainerSimulator> pendingReduces =
      new LinkedList<ContainerSimulator>();

  // pending failed reduces
  private LinkedList<ContainerSimulator> pendingFailedReduces =
      new LinkedList<ContainerSimulator>();

  // scheduled reduces
  private LinkedList<ContainerSimulator> scheduledReduces =
      new LinkedList<ContainerSimulator>();

  // assigned reduces
  private Map<ContainerId, ContainerSimulator> assignedReduces =
      new HashMap<ContainerId, ContainerSimulator>();

  // all maps & reduces
  private LinkedList<ContainerSimulator> allMaps =
      new LinkedList<ContainerSimulator>();
  private LinkedList<ContainerSimulator> allReduces =
      new LinkedList<ContainerSimulator>();

  // counters
  private int mapFinished = 0;
  private int mapTotal = 0;
  private int reduceFinished = 0;
  private int reduceTotal = 0;
  // finished
  private boolean isFinished = false;

  public final Logger LOG = Logger.getLogger(UnManagedMRAMSimulatorForRealRM.class);

  public void init(int id, int heartbeatInterval,
      List<ContainerSimulator> containerList, Configuration conf, SLSRunnerForRealRM se,
      long traceStartTime, long traceFinishTime, String user, String queue,
      boolean isTracked, String oldAppId) {
    super.init(id, heartbeatInterval, containerList, conf, se, traceStartTime,
        traceFinishTime, user, queue, isTracked, oldAppId);
    amtype = "mapreduce";

    // get map/reduce tasks
    for (ContainerSimulator cs : containerList) {
      if (cs.getType().equals("map")) {
        cs.setPriority(PRIORITY_MAP);
        pendingMaps.add(cs);
      } else if (cs.getType().equals("reduce")) {
        cs.setPriority(PRIORITY_REDUCE);
        pendingReduces.add(cs);
      }
    }
    allMaps.addAll(pendingMaps);
    allReduces.addAll(pendingReduces);
    mapTotal = pendingMaps.size();
    reduceTotal = pendingReduces.size();
    totalContainers = mapTotal + reduceTotal;
  }

  @Override
  public void firstStep() throws Exception {

    long begin = System.currentTimeMillis();

    super.firstStep();

  }

  @Override
  @SuppressWarnings("unchecked")
  protected void processResponseQueue()
      throws InterruptedException, YarnException, IOException {

    while (!responseQueue.isEmpty()) {
      AllocateResponse response = responseQueue.take();

      // check completed containers
      if (!response.getCompletedContainersStatuses().isEmpty()) {
        for (ContainerStatus cs : response.getCompletedContainersStatuses()) {
          ContainerId containerId = cs.getContainerId();
          if (cs.getExitStatus() == ContainerExitStatus.SUCCESS) {
            if (assignedMaps.containsKey(containerId)) {
              if (LOG.isDebugEnabled())
                LOG.debug(MessageFormat.format(
                    "Application {0} has one" + "mapper finished ({1}).", appId,
                    containerId));
              assignedMaps.remove(containerId);
              mapFinished++;
              finishedContainers++;
            } else if (assignedReduces.containsKey(containerId)) {
              if (LOG.isDebugEnabled())
                LOG.debug(MessageFormat.format(
                    "Application {0} has one" + "reducer finished ({1}).",
                    appId, containerId));
              assignedReduces.remove(containerId);
              reduceFinished++;
              finishedContainers++;
            }
          } else {
            // container to be killed
            if (assignedMaps.containsKey(containerId)) {
              if(LOG.isDebugEnabled())
                LOG.debug(MessageFormat.format(
                    "Application {0} has one " + "mapper ({1}) exitcode({2}).", appId,
                    containerId,cs.getExitStatus()));
              pendingFailedMaps.add(assignedMaps.remove(containerId));
            } else if (assignedReduces.containsKey(containerId)) {
              if(LOG.isDebugEnabled())
                LOG.debug(MessageFormat.format(
                    "Application {0} has one " + "reducer ({1}) exitcode({2}).", appId,
                    containerId,cs.getExitStatus()));
              pendingFailedReduces.add(assignedReduces.remove(containerId));
            }
          }
        }
      }

      // check finished
      if ((mapFinished == mapTotal)
          && (reduceFinished == reduceTotal)) {
        isFinished = true;
        return;
      }

      // check allocated containers
      for (Container container : response.getAllocatedContainers()) {
        if (!scheduledMaps.isEmpty()) {
          ContainerSimulator cs = scheduledMaps.remove();
          if (LOG.isDebugEnabled())
            LOG.debug(MessageFormat.format(
                "Application {0} starts a " + "launch a mapper ({1}).", appId,
                container.getId()));
          assignedMaps.put(container.getId(), cs);
          se.getNmMap().get(container.getNodeId()).addNewContainer(container,
              cs.getLifeTime());
        } else if (!this.scheduledReduces.isEmpty()) {
          ContainerSimulator cs = scheduledReduces.remove();
          if (LOG.isDebugEnabled())
            LOG.debug(MessageFormat.format(
                "Application {0} starts a " + "launch a reducer ({1}).", appId,
                container.getId()));
          assignedReduces.put(container.getId(), cs);
          se.getNmMap().get(container.getNodeId()).addNewContainer(container,
              cs.getLifeTime());
        }
      }
    }
  }

  @Override
  protected void sendContainerRequest()
      throws YarnException, IOException, InterruptedException {
    if (isFinished) {
      return;
    }
    // send out request
    List<ResourceRequest> ask = null;
    if (mapFinished != mapTotal) {
      // map phase
      if (!pendingMaps.isEmpty()) {
        ask = packageRequests(pendingMaps, PRIORITY_MAP);
        if (LOG.isDebugEnabled())
          LOG.debug(MessageFormat
              .format("Application {0} sends out " + "request for {1} mappers.",
                  appId, pendingMaps.size()));
        scheduledMaps.addAll(pendingMaps);
        pendingMaps.clear();
      } else if (!pendingFailedMaps.isEmpty() && scheduledMaps.isEmpty()) {
        ask = packageRequests(pendingFailedMaps, PRIORITY_MAP);
        if (LOG.isDebugEnabled())
          LOG.debug(MessageFormat.format(
              "Application {0} sends out " + "requests for {1} failed mappers.",
              appId, pendingFailedMaps.size()));
        scheduledMaps.addAll(pendingFailedMaps);
        pendingFailedMaps.clear();
      }
    } else if (reduceFinished != reduceTotal) {
      // reduce phase
      if (!pendingReduces.isEmpty()) {
        ask = packageRequests(pendingReduces, PRIORITY_REDUCE);
        if (LOG.isDebugEnabled())
          LOG.debug(MessageFormat.format(
              "Application {0} sends out " + "requests for {1} reducers.",
              appId, pendingReduces.size()));
        scheduledReduces.addAll(pendingReduces);
        pendingReduces.clear();
      } else if (!pendingFailedReduces.isEmpty() && scheduledReduces
          .isEmpty()) {
        ask = packageRequests(pendingFailedReduces, PRIORITY_REDUCE);
        if (LOG.isDebugEnabled())
          LOG.debug(MessageFormat.format(
              "Application {0} sends out " + "request for {1} failed reducers.",
              appId, pendingFailedReduces.size()));
        scheduledReduces.addAll(pendingFailedReduces);
        pendingFailedReduces.clear();
      }
    }

    if (ask == null) {
      ask = new ArrayList<ResourceRequest>();
    }

    final AllocateRequest request = createAllocateRequest(ask);
    if (totalContainers == 0) {
      request.setProgress(1.0f);
    } else {
      request.setProgress((float) finishedContainers / totalContainers);
    }
    while (true) {
      try {
        AllocateResponse response =
            ugi.doAs(new PrivilegedExceptionAction<AllocateResponse>() {
              @Override public AllocateResponse run() throws Exception {
                return amRMClient.allocate(request);
              }
            });
        if (response != null) {
          responseQueue.put(response);
        }
        break;
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

  }

  @Override
  protected void checkStop() {
    if (isFinished) {
      super.setEndTime(System.currentTimeMillis());
    }
  }

}
