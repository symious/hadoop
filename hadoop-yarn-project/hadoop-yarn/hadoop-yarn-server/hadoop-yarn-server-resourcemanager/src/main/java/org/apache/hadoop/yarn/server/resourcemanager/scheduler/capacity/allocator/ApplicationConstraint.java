package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.allocator;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;

import org.apache.hadoop.conf.Configuration;

public interface ApplicationConstraint {

  ContainerAllocation check(FiCaSchedulerNode node,
      SchedulerRequestKey schedulerKey, FiCaSchedulerApp application,
      ActivitiesManager activitiesManager);

  void initialize(Configuration conf);
}