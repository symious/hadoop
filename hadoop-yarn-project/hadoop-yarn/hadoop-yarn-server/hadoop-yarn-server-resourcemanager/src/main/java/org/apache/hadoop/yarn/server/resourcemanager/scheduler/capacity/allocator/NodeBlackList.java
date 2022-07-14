package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.allocator;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesLogger;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityDiagnosticConstant;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityLevel;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSAMContainerLaunchDiagnosticsConstants;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeBlackList implements ApplicationConstraint {
  private static final Logger LOG =
      LoggerFactory.getLogger(NodeBlackList.class);

  @Override
  public ContainerAllocation check(FiCaSchedulerNode node,
      SchedulerRequestKey schedulerKey, FiCaSchedulerApp application,
      ActivitiesManager activitiesManager) {
    if (SchedulerAppUtils.isPlaceBlacklisted(application, node, LOG)) {
      application.updateAppSkipNodeDiagnostics(
          CSAMContainerLaunchDiagnosticsConstants.SKIP_AM_ALLOCATION_IN_BLACK_LISTED_NODE);
      ActivitiesLogger.APP.recordSkippedAppActivityWithoutAllocation(
          activitiesManager, node, application, schedulerKey,
          ActivityDiagnosticConstant.NODE_IS_BLACKLISTED,
          ActivityLevel.NODE);
      return ContainerAllocation.APP_SKIPPED;
    }
    return null;
  }

  @Override
  public void initialize(Configuration conf) {
  }
}