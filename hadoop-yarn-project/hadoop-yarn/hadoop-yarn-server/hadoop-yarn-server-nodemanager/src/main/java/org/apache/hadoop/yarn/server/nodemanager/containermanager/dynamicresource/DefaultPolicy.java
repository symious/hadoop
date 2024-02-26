package org.apache.hadoop.yarn.server.nodemanager.containermanager.dynamicresource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.thirdparty.com.google.common.cache.Cache;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerUpdateType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainerMetrics;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class DefaultPolicy implements Policy {
  private static final Logger LOG =
      LoggerFactory.getLogger(DefaultPolicy.class);

  private long minInterval;
  private Cache<Object, Object> cache;
  private long minAllocationMb;
  private long launchTimeThreshold;

  @Override
  public ContainerAdjustment apply(Container container) {
    ContainerId containerId = container.getContainerId();
    ContainerMetrics containerMetrics =
        ContainerMetrics.getContainerMetrics(containerId);
    if (containerMetrics == null || (
        Time.now() - container.getContainerLaunchTime() < launchTimeThreshold)) {
      return null;
    }

    long max = (long) containerMetrics.minMax.max();
    long latest = containerMetrics.latestMemoryMbs.value();
    long init = containerMetrics.initMemoryMbs.value();
    long limit = containerMetrics.pMemLimitMbs.value();

    LOG.debug("MemoryUsage for container: " + containerId + ". max: " + max
        + " latest: " + latest + " init: " + init + " limit: " + limit);

    if (Time.now() - container.getLastChangeResourceTime() < minInterval) {
      return null;
    }
    if (limit < max) {
      long target = Math.min(init, max);
      if (target == limit) {
        return null;
      }
      long normalized = ResourceCalculator
          .roundUp(Math.max(minAllocationMb, target), minAllocationMb);

      LOG.info("Increase resource from " + limit + " to " + normalized
          + " for container: " + containerId);

      return new ContainerAdjustment(containerId, container,
          ContainerUpdateType.INCREASE_RESOURCE,
          container.getContainerTokenIdentifier().getResource(),
          normalized - limit);
    } else {
      if ((double) max / (double) limit > 0.8) {
        return null;
      } else {
        long target = limit - (limit - max) / 2;
        long normalized = ResourceCalculator
            .roundUp(Math.max(minAllocationMb, target), minAllocationMb);
        LOG.debug(
            "Before normalized: " + target + " After normalized: " + normalized
                + " for container: " + containerId);
        if (normalized == limit) {
          return null;
        }
        LOG.info("Decrease resource from " + limit + " to " + normalized
            + " for container: " + containerId);

        return new ContainerAdjustment(containerId, container,
            ContainerUpdateType.DECREASE_RESOURCE,
            container.getContainerTokenIdentifier().getResource(),
            normalized - limit);
      }
    }
  }

  @Override
  public void init(Configuration conf) {
    this.minInterval = conf.getLong(
        YarnConfiguration.NM_DYNAMIC_ADJUSTMENT_DEFAULT_POLICY_INTERVAL_MS,
        YarnConfiguration.DEFAULT_NM_DYNAMIC_ADJUSTMENT_DEFAULT_POLICY_INTERVAL_MS);
    this.cache = CacheBuilder.newBuilder().expireAfterWrite(minInterval,
        TimeUnit.MILLISECONDS).build();
    this.minAllocationMb =
        conf.getLong(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
    this.launchTimeThreshold = conf.getLong(
        YarnConfiguration.NM_DYNAMIC_ADJUSTMENT_CONTAINER_LAUNCH_TIME_THRESHOLD,
        YarnConfiguration.DEFAULT_NM_DYNAMIC_ADJUSTMENT_CONTAINER_LAUNCH_TIME_THRESHOLD);
  }
}
