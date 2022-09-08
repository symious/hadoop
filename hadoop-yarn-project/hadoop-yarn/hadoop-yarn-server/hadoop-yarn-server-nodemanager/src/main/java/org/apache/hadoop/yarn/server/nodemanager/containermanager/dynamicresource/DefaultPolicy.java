package org.apache.hadoop.yarn.server.nodemanager.containermanager.dynamicresource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.thirdparty.com.google.common.cache.Cache;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerUpdateType;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.UpdateContainerRequest;
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

  @Override
  public UpdateContainerRequest apply(Container container) {
    ContainerId containerId = container.getContainerId();
    ContainerMetrics containerMetrics =
        ContainerMetrics.getContainerMetrics(containerId);
    if (containerMetrics == null)
      return null;

    long max = (long) containerMetrics.minMax.max();
    long latest = containerMetrics.latestMemoryMbs;
    long init = containerMetrics.initMemoryMbs;
    long deserved = containerMetrics.pMemLimitMbs.value();

    LOG.debug("MemoryUsage for container: " + containerId + ". max: " + max
        + " latest: " + latest + " init: " + init + " deserved: " + deserved);

    if (latest <= max) {
      if (cache.getIfPresent(containerId) != null) {
        return null;
      }
      if (deserved < max) {
        long target = Math.min(init, max);
        if (target == deserved) {
          return null;
        }
        cache.put(containerId, System.currentTimeMillis());
        LOG.info("Increase resource from " + deserved + " to " + target
            + " for container: " + containerId);
        return UpdateContainerRequest
            .newInstance(container.getContainerTokenIdentifier().getVersion(),
                containerId, ContainerUpdateType.INCREASE_RESOURCE, Resource
                    .newInstance(target, containerMetrics.cpuVcoreLimit.value()),
                container.getContainerTokenIdentifier().getExecutionType());
      } else {
        if ((double)max / (double)deserved > 0.8) {
          return null;
        } else {
          long target = deserved - (deserved - max) / 2;
          long normalized = ResourceCalculator
              .roundUp(Math.max(minAllocationMb, target), minAllocationMb);
          LOG.debug("Before normalized: " + target + " After normalized: " + normalized
              + " for container: " + containerId);
          if (normalized == deserved) {
            return null;
          }
          cache.put(containerId, System.currentTimeMillis());
          LOG.info("Decrease resource from " + deserved + " to " + normalized
              + " for container: " + containerId);
          return UpdateContainerRequest
              .newInstance(container.getContainerTokenIdentifier().getVersion(),
                  containerId, ContainerUpdateType.DECREASE_RESOURCE, Resource
                      .newInstance(normalized,
                          containerMetrics.cpuVcoreLimit.value()),
                  container.getContainerTokenIdentifier().getExecutionType());
        }
      }
    } else {
      return null;
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
  }
}
