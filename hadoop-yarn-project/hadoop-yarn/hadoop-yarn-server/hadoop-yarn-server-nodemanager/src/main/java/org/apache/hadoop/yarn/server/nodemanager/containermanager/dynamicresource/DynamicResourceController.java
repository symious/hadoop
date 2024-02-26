package org.apache.hadoop.yarn.server.nodemanager.containermanager.dynamicresource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.UpdateContainerRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.ContainerType;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DynamicResourceController extends Thread {

  private final static Logger LOG =
      LoggerFactory.getLogger(DynamicResourceController.class);

  private Context context;
  private Policy policy;
  private long monitoringInterval;
  private long minAllocationMb;

  public DynamicResourceController(Configuration conf, Context context) {
    super("DynamicMemoryController");
    this.context = context;
    policy = getPolicy(conf);
    policy.init(conf);
    monitoringInterval =
        conf.getLong(YarnConfiguration.NM_DYNAMIC_ADJUSTMENT_INTERVAL_MS,
            YarnConfiguration.DEFAULT_NM_DYNAMIC_ADJUSTMENT_INTERVAL_MS);
    minAllocationMb =
        conf.getLong(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
  }

  private Policy getPolicy(Configuration conf) {
    Class<? extends Policy> policyClass =
        conf.getClass(YarnConfiguration.NM_DYNAMIC_ADJUSTMENT_POLICY_CLASS,
            DefaultPolicy.class, Policy.class);
    return ReflectionUtils.newInstance(policyClass, conf);
  }

  @Override
  public void run() {
    List<ContainerAdjustment> adjustments = new ArrayList<>();
    while (true) {
      for (Container container : context.getContainers().values()) {
        if (!container.isRunning() || container.getContainerTokenIdentifier()
            .getContainerType().equals(ContainerType.APPLICATION_MASTER)) {
          continue;
        }
        ContainerAdjustment containerAdjustment = policy.apply(container);
        if (containerAdjustment != null) {
          adjustments.add(containerAdjustment);
        }
      }
      mergeIncreaseAndDecrease(adjustments);
      for (ContainerAdjustment ca : adjustments) {
        Resource target = Resources.add(ca.getOriginalResource(),
            Resource.newInstance(ca.deltaMemory, 0));
        UpdateContainerRequest ucr = UpdateContainerRequest.newInstance(
            ca.getContainer().getContainerTokenIdentifier().getVersion(),
            ca.getContainerId(), ca.getContainerUpdateType(), target,
            ca.getContainer().getContainerTokenIdentifier().getExecutionType());

        context.getTobeUpdatedContainers().put(ca.getContainerId(), ucr);
      }
      adjustments.clear();
      try {
        Thread.sleep(monitoringInterval);
      } catch (InterruptedException e) {
        LOG.warn("{} is interrupted. Exiting.",
            DynamicResourceController.class.getName());
        break;
      }
    }
  }

  public void mergeIncreaseAndDecrease(List<ContainerAdjustment> adjustments) {
    long increaseSum = 0, decreaseSum = 0;
    List<ContainerAdjustment> decreaseSet = new ArrayList<>();
    for (ContainerAdjustment ca : adjustments) {
      switch (ca.getContainerUpdateType()) {
        case INCREASE_RESOURCE:
          increaseSum += ca.deltaMemory;
          break;
        case DECREASE_RESOURCE:
          decreaseSum += ca.deltaMemory;
          decreaseSet.add(ca);
          break;
      }
    }
    /*
      if increaseSum != 0 & decreaseSum != 0 means that
      there must cancel some part decrease request, so print how much.
     */
    if (increaseSum != 0 && decreaseSum != 0) {
      LOG.info(
          "Merge action cancel " + (increaseSum >= Math.abs(decreaseSum) ?
              Math.abs(decreaseSum) : increaseSum)
              + "Mb memory decrease request.");
    }
    if (increaseSum >= Math.abs(decreaseSum)) {
      adjustments.removeAll(decreaseSet);
      return;
    }
    long delta = increaseSum;
    long stepFactor = minAllocationMb;
    Iterator<ContainerAdjustment> iter = decreaseSet.iterator();
    while (delta > 0) {
      if (!iter.hasNext()) {
        iter = decreaseSet.iterator();
      }
      ContainerAdjustment ca = iter.next();
      delta -= stepFactor;
      if (Math.abs(ca.deltaMemory) <= stepFactor) {
        adjustments.remove(ca);
        iter.remove();
        continue;
      }
      ca.deltaMemory += stepFactor;
    }
  }
}
