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
import java.util.concurrent.ConcurrentSkipListSet;

public class CompositeWeightOrderingPolicy<S extends SchedulableEntity> extends AbstractComparatorOrderingPolicy<S> {

  private static final Logger LOG =
      LoggerFactory.getLogger(CompositeWeightOrderingPolicy.class);

  private String queueName;
  private long cacheTime;

  //global scheduler will have multiple threads, update visibility
  private volatile long lastUpdateTime;

  //default: 60
  private double highFlagPriority;

  //default: 100 TB
  private double pendingFlagMemory;

  //default: 120 minutes
  private double pendingFlagTime;

  //default: 0.6
  private double priorityWeightFactor;

  //default: 0.2
  private double pendingMemoryWeightFactor;

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

  public double getPendingFlagMemory() {
    return pendingFlagMemory;
  }

  public void setPendingFlagMemory(double pendingFlagMemory) {
    this.pendingFlagMemory = pendingFlagMemory;
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

  public double getPendingMemoryWeightFactor() {
    return pendingMemoryWeightFactor;
  }

  public void setPendingMemoryWeightFactor(double pendingMemoryWeightFactor) {
    this.pendingMemoryWeightFactor = pendingMemoryWeightFactor;
  }

  public double getPendingTimeWeightFactor() {
    return pendingTimeWeightFactor;
  }

  public void setPendingTimeWeightFactor(double pendingTimeWeightFactor) {
    this.pendingTimeWeightFactor = pendingTimeWeightFactor;
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
      // (pending_resources / pending_flag_resources) * n +
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

        double r1_pending_resources_weight =
            r1.getSchedulingResourceUsage()
                .getCachedDemand(CommonNodeLabelsManager.ANY)
                .getMemorySize() / pendingFlagMemory;
        r1_pending_resources_weight =
            (r1_pending_resources_weight < 1) ? r1_pending_resources_weight : 1;
        r1_pending_resources_weight =
            r1_pending_resources_weight * pendingMemoryWeightFactor;

        double r2_pending_resources_weight =
            r2.getSchedulingResourceUsage()
                .getCachedDemand(CommonNodeLabelsManager.ANY)
                .getMemorySize() / pendingFlagMemory;
        r2_pending_resources_weight =
            (r2_pending_resources_weight < 1) ? r2_pending_resources_weight : 1;
        r2_pending_resources_weight =
            r2_pending_resources_weight * pendingMemoryWeightFactor;

        long currentTimeMillis = System.currentTimeMillis();
        double r1_pending_time_weight =
            (currentTimeMillis - r1.getStartTime()) / pendingFlagTime;
        r1_pending_time_weight =
            (r1_pending_time_weight < 1) ? r1_pending_time_weight : 1;
        r1_pending_time_weight =
            r1_pending_time_weight * pendingTimeWeightFactor;

        double r2_pending_time_weight =
            (currentTimeMillis - r2.getStartTime()) / pendingFlagTime;
        r2_pending_time_weight =
            (r2_pending_time_weight < 1) ? r2_pending_time_weight : 1;
        r2_pending_time_weight =
            r2_pending_time_weight * pendingTimeWeightFactor;

        double r1_composite_weight =
            r1_priority_weight + r1_pending_resources_weight +
                r1_pending_time_weight;

        double r2_composite_weight =
            r2_priority_weight + r2_pending_resources_weight +
                r2_pending_time_weight;

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
    comparators.add(new WeightComparator());
    comparators.add(new StartTimeComparator());
    weightComparator = new CompoundComparator(
      comparators
      );
    this.comparator = weightComparator;
    this.schedulableEntities = new ConcurrentSkipListSet<S>(comparator);
  }

  @Override
  public Iterator<S> getAssignmentIterator(IteratorSelector sel) {
    long now = System.currentTimeMillis();
    if(now - lastUpdateTime > cacheTime){
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
    this.highFlagPriority = Double.parseDouble(conf.get("highFlagPriority"));
    this.pendingFlagMemory = Double.parseDouble(conf.get("pendingFlagMemory"));
    this.pendingFlagTime = Double.parseDouble(conf.get("pendingFlagTime"));
    this.priorityWeightFactor =
        Double.parseDouble(conf.get("priorityWeightFactor"));
    this.pendingMemoryWeightFactor =
        Double.parseDouble(conf.get("pendingMemoryWeightFactor"));

    if (this.priorityWeightFactor + this.priorityWeightFactor < 1) {
      this.pendingTimeWeightFactor =
          1.0 - (priorityWeightFactor + pendingMemoryWeightFactor);
    } else {
      this.priorityWeightFactor =
          CapacitySchedulerConfiguration.DEFAULT_APP_PRIORITY_WEIGHT_FACTOR;
      this.pendingMemoryWeightFactor =
          CapacitySchedulerConfiguration.DEFAULT_APP_PENDING_MEMORY_WEIGHT_FACTOR;
      this.pendingTimeWeightFactor =
          CapacitySchedulerConfiguration.DEFAULT_APP_PENDING_TIME_WEIGHT_FACTOR;
      LOG.warn("Invalid WeightFactor config, fall back to default config, " +
              "priorityWeightFactor: " + this.priorityWeightFactor +
              " ,pendingMemoryWeightFactor: " + this.pendingMemoryWeightFactor +
          " ,pendingTimeWeightFactor: " + pendingTimeWeightFactor);
    }
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
