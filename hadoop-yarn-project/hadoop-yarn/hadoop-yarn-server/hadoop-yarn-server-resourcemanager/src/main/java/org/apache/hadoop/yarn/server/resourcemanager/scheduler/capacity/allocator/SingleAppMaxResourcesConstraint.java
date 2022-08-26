package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.allocator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesLogger;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityDiagnosticConstant;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityLevel;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSAMContainerLaunchDiagnosticsConstants;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class SingleAppMaxResourcesConstraint implements ApplicationConstraint {

  private static final Logger LOG =
      LoggerFactory.getLogger(SingleAppMaxResourcesConstraint.class);

  private RMContext rmContext;
  private int logMinute;

  @Override
  public synchronized ContainerAllocation check(FiCaSchedulerNode node,
      SchedulerRequestKey schedulerKey, FiCaSchedulerApp application,
      ActivitiesManager activitiesManager) {

    // Check whether the resource used by the app exceeds the maximum
    // resource limit for a single app in the queue

    LeafQueue leafQueue = application.getCSLeafQueue();
    String queueName = leafQueue.getQueueName();

    boolean enableCheckAppMaxResources = leafQueue.getEnableCheckAppMaxResources();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Queue: " + queueName + " ,enableCheckAppMaxResources: " +
          enableCheckAppMaxResources);
    }

    if (enableCheckAppMaxResources) {
      long start = System.nanoTime();
      ResourceUsage appResUsageReport =
          application.getAppAttemptResourceUsage();
      int runningCpuVcores =
          appResUsageReport.getAllUsed().getVirtualCores();
      long runningMemoryMB =
          appResUsageReport.getAllUsed().getMemorySize();
      int reservedCpuVcores =
          appResUsageReport.getAllReserved().getVirtualCores();
      long reservedMemoryMB =
          appResUsageReport.getAllReserved().getMemorySize();
      Resource newAskResource =
          application.getPendingAsk(schedulerKey, ResourceRequest.ANY)
              .getPerAllocationResource();
      if (LOG.isDebugEnabled()) {
        LOG.debug("applicationId: " + application.getApplicationId() +
            " ,askResource: " + newAskResource);
      }
      int newAskCpuVcores = newAskResource.getVirtualCores();
      long newAskMemoryMB = newAskResource.getMemorySize();
      int appPlanCpuVcores =
          runningCpuVcores + reservedCpuVcores + newAskCpuVcores;
      long appPlanMemoryMB =
          runningMemoryMB + reservedMemoryMB + newAskMemoryMB;
      boolean canKillApp = leafQueue.getKillAppWhenOverResources();
      int queuePerAppMaxVcores = leafQueue.getQueuePerAppMaxVcores();
      long queuePerAppMaxMemoryMB = leafQueue.getQueuePerAppMaxMemoryMB();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Queue: " + queueName + " ,queuePerAppMaxVcores: " +
            queuePerAppMaxVcores + " ,queuePerAppMaxMemoryMB: " +
            queuePerAppMaxMemoryMB + " ,canKillApp: " + canKillApp);
      }
      if (appPlanCpuVcores > queuePerAppMaxVcores ||
          appPlanMemoryMB > queuePerAppMaxMemoryMB) {
        ApplicationId appId = application.getApplicationId();
        String message =
            " queue: " + leafQueue.getQueuePath() + " ,application: " + appId +
                " plan to use resources: [" +
                runningCpuVcores + " VCores, " + runningMemoryMB +
                " MB], reach queue max resources limit: " + "[" +
                queuePerAppMaxVcores + " VCores, " + queuePerAppMaxMemoryMB +
                " MB], can't assign new containers!";
        if (Calendar.getInstance().get(Calendar.MINUTE) != logMinute) {
          LOG.warn(message);
          logMinute = Calendar.getInstance().get(Calendar.MINUTE);
        }
        if (canKillApp) {
          this.rmContext.getDispatcher().getEventHandler().handle(
              new RMAppEvent(appId, RMAppEventType.KILL, message));
        }
        application.updateAppSkipNodeDiagnostics(
            CSAMContainerLaunchDiagnosticsConstants.SKIP_CONTAINER_ALLOCATION_DUE_TO_APP_MAX_RESOURCES_CONSTRAINT);
        ActivitiesLogger.APP
            .recordSkippedAppActivityWithoutAllocation(activitiesManager, node,
                application, schedulerKey,
                ActivityDiagnosticConstant.APPLICATION_OVER_RESOURCE,
                ActivityLevel.APP);
        long end1 = System.nanoTime();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Check single app resources limit cost time: " +
              (end1 - start) / 1000 + " us!");
        }
        return ContainerAllocation.QUEUE_CONSTRAINT_SKIPPED;
      }
      long end2 = System.nanoTime();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Check single app resources limit cost time: " +
            (end2 - start) / 1000 + " us!");
      }
    }
    return null;
  }

  @Override
  public void initialize(Configuration conf) {
  }

  public void setRMContext(RMContext rmContext){
    this.rmContext = rmContext;
  }
}
