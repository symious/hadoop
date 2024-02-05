package org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class IntraQueueEnsureAMResourceSelector
    extends PreemptionCandidatesSelector {

  final CapacitySchedulerPreemptionContext context;

  private static final Logger LOG =
      LoggerFactory.getLogger(IntraQueueEnsureAMResourceSelector.class);

  IntraQueueEnsureAMResourceSelector(
      CapacitySchedulerPreemptionContext preemptionContext) {
    super(preemptionContext);
    context = preemptionContext;
  }

  @Override
  public Map<ApplicationAttemptId, Set<RMContainer>> selectCandidates(
      Map<ApplicationAttemptId, Set<RMContainer>> selectedCandidates,
      Resource clusterResource, Resource totalPreemptedResourceAllowed) {

    long start = System.currentTimeMillis();

    Map<ApplicationAttemptId, Set<RMContainer>> curCandidates = new HashMap<>();

    // 1. Calculate the resource to obtain per partition
    Map<String, Map<TempQueuePerPartition, Resource>> resToObtainByQueue =
        computeIntraQueuePreemptResourceForAMDemand(clusterResource);
    if (MapUtils.isEmpty(resToObtainByQueue)) {
      return curCandidates;
    }

    // 2. Loop all leaf queues to select containers for preemption
    preemptNonAMResources(selectedCandidates, curCandidates, clusterResource,
        resToObtainByQueue);

    long end = System.currentTimeMillis();
    if (LOG.isDebugEnabled()) {
      LOG.debug("selectCandidates " + curCandidates + " ,cost time: " +
          (end - start) + "ms!");
    }

    return curCandidates;
  }


  private void preemptNonAMResources(
      Map<ApplicationAttemptId, Set<RMContainer>> selectedCandidates,
      Map<ApplicationAttemptId, Set<RMContainer>> curCandidates,
      Resource clusterResource,
      Map<String, Map<TempQueuePerPartition, Resource>> resToObtainByQueue) {

    for (String queueName : resToObtainByQueue.keySet()) {

      LeafQueue leafQueue = preemptionContext.getQueueByPartition(queueName,
          RMNodeLabelsManager.NO_LABEL).leafQueue;

      if (leafQueue.getIntraQueuePreemptionDisabled()) {
        continue;
      }

      Map<TempQueuePerPartition, Resource> resToObtainByPartition =
          resToObtainByQueue.get(queueName);
      if (LOG.isDebugEnabled()) {
        LOG.debug("queueName: " + queueName + " ,resToObtainByPartition: " +
            resToObtainByPartition);
      }

      Iterator<FiCaSchedulerApp> desc =
          leafQueue.getOrderingPolicy().getPreemptionIterator();

      leafQueue.getReadLock().lock();
      try {
        while (desc.hasNext()) {
          if (MapUtils.isEmpty(resToObtainByPartition)) {
            break;
          }
          FiCaSchedulerApp app = desc.next();
          if (skipHighestAppPreemption(app)) {
            continue;
          }
          preemptFromLeastStarvedApp(app, selectedCandidates,
              curCandidates, clusterResource, resToObtainByPartition);
        }
      } finally {
        leafQueue.getReadLock().unlock();
      }
    }
  }

  private Map<String, Map<TempQueuePerPartition, Resource>> computeIntraQueuePreemptResourceForAMDemand(
      Resource clusterResource) {

    Map<String, Map<TempQueuePerPartition, Resource>> resToObtainByQueue =
        new HashMap<>();

    RMContext rmContext = preemptionContext.getRMContext();
    Map<ApplicationId, RMApp> rmApps = rmContext.getRMApps();

    // Loop all leaf queues, get all queue partition preemptResourceForAMDemand
    for (String queueName : preemptionContext.getLeafQueueNames()) {

      LeafQueue leafQueue = preemptionContext.getQueueByPartition(queueName,
          RMNodeLabelsManager.NO_LABEL).leafQueue;
      if (leafQueue.getIntraQueuePreemptionDisabled()) {
        continue;
      }

      // We do not need preemption for a single app
      Collection<FiCaSchedulerApp> apps = leafQueue.getAllApplications();
      int allAppsCount = apps.size();
      if (allAppsCount <= 1) {
        continue;
      }

      for (FiCaSchedulerApp app : apps) {
        try {
          RMApp rmApp = rmApps.get(app.getApplicationId());
          if (LOG.isDebugEnabled()) {
            LOG.debug(
                "queueName: " + queueName + " ,app: " + app.getApplicationId() +
                    " ,app.isWaitingForAMContainer(): " +
                    app.isWaitingForAMContainer() +
                    " ,submitTime: " + rmApp.getSubmitTime() + " ,past time: " +
                    (System.currentTimeMillis() - rmApp.getSubmitTime()));
          }

          if (!app.isRecovering() && app.isWaitingForAMContainer()) {
            ResourceRequest resourceRequest =
                rmApp.getAMResourceRequests().get(0);
            if (resourceRequest != null) {
              Resource pendingAMResource = resourceRequest.getCapability();
              String requestPartition = StringUtils
                  .isNotBlank(resourceRequest.getNodeLabelExpression()) ?
                  resourceRequest.getNodeLabelExpression().trim() :
                  RMNodeLabelsManager.NO_LABEL;
              if (Resources.greaterThan(rc, clusterResource, pendingAMResource,
                  Resources.none())) {
                TempQueuePerPartition tq =
                    context.getQueueByPartition(queueName, requestPartition);

                Map<TempQueuePerPartition, Resource> resToObtainByPartition;
                if (!resToObtainByQueue.containsKey(queueName)) {
                  resToObtainByPartition = new HashMap<>();
                } else {
                  resToObtainByPartition = resToObtainByQueue.get(queueName);
                }
                Resource resToObtain = resToObtainByPartition.get(tq);
                if (resToObtain == null) {
                  resToObtain = Resource.newInstance(0, 0);
                }
                Resource newPendingResource =
                    Resources.add(resToObtain, pendingAMResource);
                resToObtainByPartition.put(tq, newPendingResource);
                resToObtainByQueue.put(queueName, resToObtainByPartition);
                if (LOG.isDebugEnabled()) {
                  LOG.debug("queueName: " + queueName + " ,requestPartition: " +
                      requestPartition + " ,newPendingResource: " +
                      newPendingResource);
                }
              }
            }
          }
        } catch (Exception e) {
          LOG.error("unKnown exception for app: " + app.getId(), e);
        }
      }
    }

    //Fix the resources that need to be preempted based on the partition’s available resources.
    fixPreemptResourceOnPartitionAvailResource(resToObtainByQueue);

    return resToObtainByQueue;
  }

  private void fixPreemptResourceOnPartitionAvailResource(
      Map<String, Map<TempQueuePerPartition, Resource>> resToObtainByQueue) {
    for (Map.Entry<String, Map<TempQueuePerPartition, Resource>> entry : resToObtainByQueue
        .entrySet()) {

      Map<TempQueuePerPartition, Resource> queuePartitionNeedResourceMap =
          entry.getValue();

      Iterator<Map.Entry<TempQueuePerPartition, Resource>> iterator =
          queuePartitionNeedResourceMap.entrySet().iterator();

      while (iterator.hasNext()) {
        Map.Entry<TempQueuePerPartition, Resource>
            queuePerPartitionResourceEntry = iterator.next();
        TempQueuePerPartition tq =
            queuePerPartitionResourceEntry.getKey();
        Resource sumPreemptQueuePartitionResourceForAM =
            queuePerPartitionResourceEntry.getValue();
        Resource availableResource =
            Resources.subtract(tq.getGuaranteed(), tq.getUsed());

        if (rc.isAnyMajorResourceZeroOrNegative(availableResource)) {
          availableResource = Resource.newInstance(0, 0);
        }

        Resource toPreemptionAfterAllocate =
            Resources.subtract(sumPreemptQueuePartitionResourceForAM,
                availableResource);

        if (!rc.isAnyMajorResourceAboveZero(toPreemptionAfterAllocate)) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(
                "queue: " + tq.getQueueName() + " ,partition: " + tq.partition +
                    " ,sumPreemptQueuePartitionResourceForAM: " +
                    toPreemptionAfterAllocate + " less than 0");
          }
          iterator.remove();
        } else {
          Resources.subtractFrom(sumPreemptQueuePartitionResourceForAM,
              availableResource);
        }
      }
    }
  }

  private void preemptFromLeastStarvedApp(FiCaSchedulerApp app,
      Map<ApplicationAttemptId, Set<RMContainer>> selectedCandidates,
      Map<ApplicationAttemptId, Set<RMContainer>> curCandidates,
      Resource clusterResource,
      Map<TempQueuePerPartition, Resource> resToObtainByPartition) {

    List<RMContainer> liveContainers = new ArrayList<>(app.getLiveContainers());
    sortContainers(liveContainers);

    for (RMContainer c : liveContainers) {

      // skip preselected containers.
      if (CapacitySchedulerPreemptionUtils.isContainerAlreadySelected(c,
          selectedCandidates)) {
        continue;
      }

      // Skip already marked to killable containers
      if (null != preemptionContext.getKillableContainers() && preemptionContext
          .getKillableContainers().contains(c.getContainerId())) {
        continue;
      }

      // Skip AM Container from preemption for now.
      if (c.isAMContainer()) {
        continue;
      }

      // Try to preempt this container
      tryPreemptContainerAndDeductResToObtain(rc, preemptionContext,
          resToObtainByPartition, c, selectedCandidates,
          curCandidates);

    }
  }


  public boolean tryPreemptContainerAndDeductResToObtain(
      ResourceCalculator rc, CapacitySchedulerPreemptionContext context,
      Map<TempQueuePerPartition, Resource> resourceToObtainByPartitions,
      RMContainer rmContainer,
      Map<ApplicationAttemptId, Set<RMContainer>> preemptMap,
      Map<ApplicationAttemptId, Set<RMContainer>> curCandidates) {
    ApplicationAttemptId attemptId = rmContainer.getApplicationAttemptId();

    // We will not account resource of a container twice or more
    if (preemptMapContains(preemptMap, attemptId, rmContainer)) {
      return false;
    }

    String queueName = rmContainer.getQueueName();
    String nodePartition =
        getPartitionByNodeId(context, rmContainer.getAllocatedNode());

    TempQueuePerPartition tq =
        context.getQueueByPartition(queueName, nodePartition);

    Resource toObtainByPartition = resourceToObtainByPartitions.get(tq);
    if (null == toObtainByPartition) {
      return false;
    }

    if (rc.isAnyMajorResourceAboveZero(toObtainByPartition)) {
      Resources.subtractFrom(toObtainByPartition,
          rmContainer.getAllocatedResource());

      // When we have no more resource need to obtain, remove from map.
      if (!rc.isAnyMajorResourceAboveZero(toObtainByPartition)) {
        resourceToObtainByPartitions.remove(tq);
      }

      // Add to preemptMap
      CapacitySchedulerPreemptionUtils
          .addToPreemptMap(preemptMap, curCandidates, attemptId, rmContainer);
      return true;
    }

    return false;
  }

  private String getPartitionByNodeId(
      CapacitySchedulerPreemptionContext context, NodeId nodeId) {
    return context.getScheduler().getSchedulerNode(nodeId).getPartition();
  }

  private boolean preemptMapContains(
      Map<ApplicationAttemptId, Set<RMContainer>> preemptMap,
      ApplicationAttemptId attemptId, RMContainer rmContainer) {
    Set<RMContainer> rmContainers = preemptMap.get(attemptId);
    if (null == rmContainers) {
      return false;
    }
    return rmContainers.contains(rmContainer);
  }

}
