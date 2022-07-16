package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.allocator;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesLogger;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityDiagnosticConstant;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityLevel;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSAMContainerLaunchDiagnosticsConstants;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;

import org.apache.hadoop.conf.Configuration;

public class PlacementBalance implements ApplicationConstraint {
  private int counter;
  private int skipThresold;
  private int maxAssignment;

  @Override
  public synchronized ContainerAllocation check(FiCaSchedulerNode node,
      SchedulerRequestKey schedulerKey, FiCaSchedulerApp application,
      ActivitiesManager activitiesManager) {
    if (counter > skipThresold) {
      return null;
    }
    long count = node.getNumContainers(application.getApplicationAttemptId());
    if (count < maxAssignment) {
      return null;
    }
    application.updateAppSkipNodeDiagnostics(
        CSAMContainerLaunchDiagnosticsConstants.SKIP_AM_ALLOCATION_DUE_TO_BALANCE_CONSTRAINT);
    ActivitiesLogger.APP
        .recordSkippedAppActivityWithoutAllocation(activitiesManager, node,
            application, schedulerKey,
            ActivityDiagnosticConstant.NODE_ALLOCATE_TOO_MANY_CONTAINERS,
            ActivityLevel.NODE);
    counter++;
    return ContainerAllocation.APP_SKIPPED;
  }

  @Override
  public void initialize(Configuration conf) {
    skipThresold = conf.getInt(
        YarnConfiguration.RM_APPLICATION_BALANCE_CONSTRAINTS_SKIP_THRESOLD,
        YarnConfiguration.DEFAULT_RM_APPLICATION_BALANCE_CONSTRAINTS_SKIP_THRESOLD);
    maxAssignment = conf.getInt(
        YarnConfiguration.RM_APPLICATION_BALANCE_CONSTRAINTS_MAX_ASSIGNMENT,
        YarnConfiguration.DEFAULT_RM_APPLICATION_BALANCE_CONSTRAINTS_MAX_ASSIGNMENT);
  }
}
