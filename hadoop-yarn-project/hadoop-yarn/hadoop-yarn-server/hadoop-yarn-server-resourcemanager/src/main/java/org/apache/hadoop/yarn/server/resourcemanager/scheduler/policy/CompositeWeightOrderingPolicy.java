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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListSet;

public class CompositeWeightOrderingPolicy<S extends SchedulableEntity> extends AbstractComparatorOrderingPolicy<S> {

  private static final Logger LOG =
      LoggerFactory.getLogger(CompositeWeightOrderingPolicy.class);

  private String queueName;
  private long cacheTime;

  private final static Random random = new Random(System.currentTimeMillis());
  private int fullReorderIntervalSecond;
  protected long nextFullOrderTime;

  //global scheduler will have multiple threads, update visibility
  private volatile long lastUpdateTime;

  //default: 60
  private double highFlagPriority;

  //default: 100 TB
  private double usedFlagMemory;

  //default: 120 minutes
  private double pendingFlagTime;

  //default: 0.6
  private double priorityWeightFactor;

  //default: 0.2
  private double usedMemoryWeightFactor;

  //default: 0.2
  private double pendingTimeWeightFactor;

  public String getQueueName() {
    return queueName;
  }

  public void setQueueName(String queueName) {
    this.queueName = queueName;
  }

  public long getCacheTime() {
    return cacheTime;
  }

  public void setCacheTime(long cacheTime) {
    this.cacheTime = cacheTime;
  }

  public long getLastUpdateTime() {
    return lastUpdateTime;
  }

  public void setLastUpdateTime(long lastUpdateTime) {
    this.lastUpdateTime = lastUpdateTime;
  }

  public double getHighFlagPriority() {
    return highFlagPriority;
  }

  public void setHighFlagPriority(double highFlagPriority) {
    this.highFlagPriority = highFlagPriority;
  }

  public double getUsedFlagMemory() {
    return usedFlagMemory;
  }

  public void setUsedFlagMemory(double usedFlagMemory) {
    this.usedFlagMemory = usedFlagMemory;
  }

  public double getPendingFlagTime() {
    return pendingFlagTime;
  }

  public void setPendingFlagTime(double pendingFlagTime) {
    this.pendingFlagTime = pendingFlagTime;
  }

  public double getPriorityWeightFactor() {
    return priorityWeightFactor;
  }

  public void setPriorityWeightFactor(double priorityWeightFactor) {
    this.priorityWeightFactor = priorityWeightFactor;
  }

  public double getUsedMemoryWeightFactor() {
    return usedMemoryWeightFactor;
  }

  public void setUsedMemoryWeightFactor(double usedMemoryWeightFactor) {
    this.usedMemoryWeightFactor = usedMemoryWeightFactor;
  }

  public double getPendingTimeWeightFactor() {
    return pendingTimeWeightFactor;
  }

  public void setPendingTimeWeightFactor(double pendingTimeWeightFactor) {
    this.pendingTimeWeightFactor = pendingTimeWeightFactor;
  }

  public void setFullReorderIntervalSecond(int fullReorderIntervalSecond) {
    this.fullReorderIntervalSecond = fullReorderIntervalSecond;
  }

  public void setNextFullOrderTime(long nextFullOrderTime) {
    this.nextFullOrderTime = nextFullOrderTime;
  }

  public CompoundComparator getWeightComparator() {
    return weightComparator;
  }

  public void setWeightComparator(
      CompoundComparator weightComparator) {
    this.weightComparator = weightComparator;
  }

  protected class WeightComparator implements Comparator<SchedulableEntity> {
    @Override
    public int compare(final SchedulableEntity r1, final SchedulableEntity r2) {

      // (app_priority / high_flag_priority) * m +
      // (used_resources / used_flag_resources) * n +
      // (pending_time / pending_flag_time) * q
      int r1_priority = r1.getPriority().getPriority();
      int r2_priority = r2.getPriority().getPriority();

      if (r1_priority < highFlagPriority && r2_priority < highFlagPriority) {
        double r1_priority_weight = r1_priority / highFlagPriority;
        r1_priority_weight = (r1_priority_weight < 1) ? r1_priority_weight : 1;
        r1_priority_weight = r1_priority_weight * priorityWeightFactor;

        double r2_priority_weight = r2_priority / highFlagPriority;
        r2_priority_weight = (r2_priority_weight < 1) ? r2_priority_weight : 1;
        r2_priority_weight = r2_priority_weight * priorityWeightFactor;

        double r1_used_resources_weight =
            r1.getSchedulingResourceUsage()
                .getCachedUsed(CommonNodeLabelsManager.ANY)
                .getMemorySize() / usedFlagMemory;
        r1_used_resources_weight =
            (r1_used_resources_weight < 1) ? (1.0 - r1_used_resources_weight) : 0;
        r1_used_resources_weight =
            r1_used_resources_weight * usedMemoryWeightFactor;

        double r2_used_resources_weight =
            r2.getSchedulingResourceUsage()
                .getCachedUsed(CommonNodeLabelsManager.ANY)
                .getMemorySize() / usedFlagMemory;
        r2_used_resources_weight =
            (r2_used_resources_weight < 1) ? (1.0 - r2_used_resources_weight) : 0;
        r2_used_resources_weight =
            r2_used_resources_weight * usedMemoryWeightFactor;

        double r1_pending_time_weight =
            (r1.getReOrderTime() - r1.getStartTime()) / pendingFlagTime;
        r1_pending_time_weight =
            (r1_pending_time_weight < 1) ? r1_pending_time_weight : 1;
        r1_pending_time_weight =
            r1_pending_time_weight * pendingTimeWeightFactor;

        double r2_pending_time_weight =
            (r2.getReOrderTime() - r2.getStartTime()) / pendingFlagTime;
        r2_pending_time_weight =
            (r2_pending_time_weight < 1) ? r2_pending_time_weight : 1;
        r2_pending_time_weight =
            r2_pending_time_weight * pendingTimeWeightFactor;

        double r1_composite_weight =
            r1_priority_weight + r1_used_resources_weight +
                r1_pending_time_weight;

        double r2_composite_weight =
            r2_priority_weight + r2_used_resources_weight +
                r2_pending_time_weight;

        if (LOG.isDebugEnabled()) {
          LOG.debug("appId: " + r1.getId() + " ,r1_priority_weight: "
              + r1_priority_weight + " ,r1_used_resources_weight: "
              + r1_used_resources_weight
              + " ,r1_pending_time_weight: " + r1_pending_time_weight +
              " ,r1_composite_weight: " + r1_composite_weight);

          LOG.debug("appId: " + r2.getId() + " ,r2_priority_weight: "
              + r2_priority_weight + " ,r2_used_resources_weight: "
              + r2_used_resources_weight
              + " ,r2_pending_time_weight: " + r2_pending_time_weight +
              " ,r2_composite_weight: " + r2_composite_weight);
        }

        return Double.compare(r2_composite_weight, r1_composite_weight);
      } else {
        return Integer.compare(r2_priority, r1_priority);
      }
    }

  }

  private CompoundComparator weightComparator;

  public CompositeWeightOrderingPolicy() {
    List<Comparator<SchedulableEntity>> comparators =
      new ArrayList<Comparator<SchedulableEntity>>();
    comparators.add(new InitUsedResourcesComparator());
    comparators.add(new WeightComparator());
    comparators.add(new StartTimeComparator());
    comparators.add(new FifoComparator());
    weightComparator = new CompoundComparator(
      comparators
      );
    this.comparator = weightComparator;
    this.schedulableEntities = new ConcurrentSkipListSet<S>(comparator);
  }

  protected void reorderScheduleEntities() {
    synchronized (entitiesToReorder) {
      long now = System.currentTimeMillis();
      if (now > nextFullOrderTime && fullReorderIntervalSecond > 0) {
        for (S s : schedulableEntities) {
          //only need to add apps that haven't updated time beyond fullReorderIntervalSecond
          long reOrderInterval = (now - s.getReOrderTime()) / 1000;
          if (reOrderInterval >= fullReorderIntervalSecond) {
            entitiesToReorder.put(s.getId(), s);
          }
        }
        int waitSecond = fullReorderIntervalSecond +
            random.nextInt(fullReorderIntervalSecond);
        nextFullOrderTime = now + waitSecond * 1000;
        if (LOG.isDebugEnabled()) {
          LOG.debug("queueName: " + this.queueName + " ,now: " + now +
              " ,nextFullOrderTime: " + nextFullOrderTime + " ,wait second: " +
              waitSecond);
        }
      }
      long start = System.nanoTime();
      int size = entitiesToReorder.size();
      for (Map.Entry<String, S> entry :
          entitiesToReorder.entrySet()) {
        reorderSchedulableEntity(entry.getValue());
      }
      long end = System.nanoTime();
      if (LOG.isDebugEnabled()) {
        LOG.debug("queueName: " + this.queueName + " ,reorder " + size +
            " apps, cost time: " + (end - start) / 1000 + " us!");
      }
      entitiesToReorder.clear();
    }
  }

  @Override
  public Iterator<S> getAssignmentIterator(IteratorSelector sel) {
    long now = System.currentTimeMillis();
    if (cacheTime <= 0 || (now - lastUpdateTime > cacheTime)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("queueName: " + this.queueName + " ,now: " + now +
            " ,lastUpdateTime: " + lastUpdateTime +
            " ,over cacheTime: " + cacheTime + " ,start to reorder apps!");
      }
      reorderScheduleEntities();
      lastUpdateTime = now;
    }
    return schedulableEntities.iterator();
  }

  @VisibleForTesting
  public long getAppsCacheTime() {
    return cacheTime;
  }

  @VisibleForTesting
  public void setAppsCacheTime(long cacheTime) {
    this.cacheTime = cacheTime;
  }

  @Override
  public void configure(Map<String, String> conf) {
    this.queueName = conf.get("queueName");
    this.cacheTime = Long.parseLong(conf.get("appsOrderCacheTime"));
    this.fullReorderIntervalSecond =
        Integer.parseInt(conf.get("fullReorderIntervalSecond"));
    this.nextFullOrderTime =
        System.currentTimeMillis() + (fullReorderIntervalSecond +
            random.nextInt(fullReorderIntervalSecond)) * 1000;
    this.highFlagPriority = Double.parseDouble(conf.get("highFlagPriority"));
    this.usedFlagMemory = Double.parseDouble(conf.get("usedFlagMemory"));
    this.pendingFlagTime = Double.parseDouble(conf.get("pendingFlagTime"));
    this.priorityWeightFactor =
        Double.parseDouble(conf.get("priorityWeightFactor"));
    this.usedMemoryWeightFactor =
        Double.parseDouble(conf.get("usedMemoryWeightFactor"));

    if (this.priorityWeightFactor + this.usedMemoryWeightFactor <= 1) {
      this.pendingTimeWeightFactor =
          1.0 - (priorityWeightFactor + usedMemoryWeightFactor);
    } else {
      this.priorityWeightFactor =
          CapacitySchedulerConfiguration.DEFAULT_APP_PRIORITY_WEIGHT_FACTOR;
      this.usedMemoryWeightFactor =
          CapacitySchedulerConfiguration.DEFAULT_APP_USED_MEMORY_WEIGHT_FACTOR;
      this.pendingTimeWeightFactor =
          CapacitySchedulerConfiguration.DEFAULT_APP_PENDING_TIME_WEIGHT_FACTOR;
      LOG.warn("Invalid WeightFactor config, fall back to default config, " +
              "priorityWeightFactor: " + this.priorityWeightFactor +
              " ,usedMemoryWeightFactor: " + this.usedMemoryWeightFactor +
          " ,pendingTimeWeightFactor: " + pendingTimeWeightFactor);
    }
    LOG.info("Final take effect results, " +
        "priorityWeightFactor: " + this.priorityWeightFactor +
        " ,usedMemoryWeightFactor: " + this.usedMemoryWeightFactor +
        " ,pendingTimeWeightFactor: " + pendingTimeWeightFactor);
  }

  @Override
  public void containerAllocated(S schedulableEntity,
    RMContainer r) {
      entityRequiresReordering(schedulableEntity);
    }

  @Override
  public void containerReleased(S schedulableEntity,
    RMContainer r) {
      entityRequiresReordering(schedulableEntity);
    }

  @Override
  public void demandUpdated(S schedulableEntity) {
    entityRequiresReordering(schedulableEntity);
  }

  @Override
  public String getInfo() {
    return "CompositeWeightOrderingPolicy";
  }

  @Override
  public String getConfigName() {
    return CapacitySchedulerConfiguration.COMPOSITE_WEIGHT_APP_ORDERING_POLICY;
  }

}
