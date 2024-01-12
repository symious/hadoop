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
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListSet;

public class CompositeWeightOrderingPolicy<S extends SchedulableEntity>
    extends AbstractComparatorOrderingPolicy<S> {

  private static final String APP_HIGH_FLAG_PRIORITY =
      "apps-high-flag-priority";
  private static final int DEFAULT_APP_HIGH_FLAG_PRIORITY = 60;

  private static final String APP_USED_FLAG_MEMORY = "apps-used-flag-memory";

  private static final int DEFAULT_APP_USED_FLAG_MEMORY = 100 * 1024 * 1024;

  private static final String APP_PENDING_FLAG_TIME = "apps-pending-flag-time";

  private static final int DEFAULT_APP_PENDING_FLAG_TIME = 120 * 60 * 1000;

  private static final String APP_PRIORITY_WEIGHT_FACTOR =
      "apps-priority-weight-factor";

  private static final double DEFAULT_APP_PRIORITY_WEIGHT_FACTOR = 0.6;

  private static final String APP_USED_MEMORY_WEIGHT_FACTOR =
      "apps-used-memory-weight-factor";

  private static final double DEFAULT_APP_USED_MEMORY_WEIGHT_FACTOR = 0.2;

  private static final String APP_TIME_WEIGHT_FACTOR =
      "apps-time-weight-factor";

  private static final double DEFAULT_APP_TIME_WEIGHT_FACTOR = 0.2;

  private static final String APPS_FULL_REORDER_INTERVAL_SECOND =
      "apps-full-reorder-interval-second";

  private static final Logger LOG =
      LoggerFactory.getLogger(CompositeWeightOrderingPolicy.class);

  private String queueName;

  private final static Random random = new Random(System.currentTimeMillis());

  protected long nextFullOrderTime;

  //global scheduler will have multiple threads, update visibility
  private volatile long lastUpdateTime;

  private long cacheTime = 0;

  private int fullReorderIntervalSecond = 60;

  private double highFlagPriority = DEFAULT_APP_HIGH_FLAG_PRIORITY;

  private double usedFlagMemory = DEFAULT_APP_USED_FLAG_MEMORY;

  private double pendingFlagTime = DEFAULT_APP_PENDING_FLAG_TIME;

  private double priorityWeightFactor = DEFAULT_APP_PRIORITY_WEIGHT_FACTOR;

  private double usedMemoryWeightFactor = DEFAULT_APP_USED_MEMORY_WEIGHT_FACTOR;

  private double pendingTimeWeightFactor = DEFAULT_APP_TIME_WEIGHT_FACTOR;

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

  public void setWeightComparator(CompoundComparator weightComparator) {
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

        double r1_used_resources_weight = r1.getSchedulingResourceUsage()
            .getCachedUsed(CommonNodeLabelsManager.ANY).getMemorySize()
            / usedFlagMemory;
        r1_used_resources_weight =
            (r1_used_resources_weight < 1) ? (1.0 - r1_used_resources_weight) :
                0;
        r1_used_resources_weight =
            r1_used_resources_weight * usedMemoryWeightFactor;

        double r2_used_resources_weight = r2.getSchedulingResourceUsage()
            .getCachedUsed(CommonNodeLabelsManager.ANY).getMemorySize()
            / usedFlagMemory;
        r2_used_resources_weight =
            (r2_used_resources_weight < 1) ? (1.0 - r2_used_resources_weight) :
                0;
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
            r1_priority_weight + r1_used_resources_weight
                + r1_pending_time_weight;

        double r2_composite_weight =
            r2_priority_weight + r2_used_resources_weight
                + r2_pending_time_weight;

        if (LOG.isDebugEnabled()) {
          LOG.debug("appId: " + r1.getId() + " ,r1_priority_weight: "
              + r1_priority_weight + " ,r1_used_resources_weight: "
              + r1_used_resources_weight + " ,r1_pending_time_weight: "
              + r1_pending_time_weight + " ,r1_composite_weight: "
              + r1_composite_weight);

          LOG.debug("appId: " + r2.getId() + " ,r2_priority_weight: "
              + r2_priority_weight + " ,r2_used_resources_weight: "
              + r2_used_resources_weight + " ,r2_pending_time_weight: "
              + r2_pending_time_weight + " ,r2_composite_weight: "
              + r2_composite_weight);
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
    weightComparator = new CompoundComparator(comparators);
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
        int waitSecond = fullReorderIntervalSecond + random
            .nextInt(fullReorderIntervalSecond);
        nextFullOrderTime = now + waitSecond * 1000;
        if (LOG.isDebugEnabled()) {
          LOG.debug("queueName: " + this.queueName + " ,now: " + now
              + " ,nextFullOrderTime: " + nextFullOrderTime + " ,wait second: "
              + waitSecond);
        }
      }
      long start = System.nanoTime();
      int size = entitiesToReorder.size();
      for (Map.Entry<String, S> entry : entitiesToReorder.entrySet()) {
        reorderSchedulableEntity(entry.getValue());
      }
      long end = System.nanoTime();
      if (LOG.isDebugEnabled()) {
        LOG.debug("queueName: " + this.queueName + " ,reorder " + size
            + " apps, cost time: " + (end - start) / 1000 + " us!");
      }
      entitiesToReorder.clear();
    }
  }

  @Override
  public Iterator<S> getAssignmentIterator(IteratorSelector sel) {
    long now = System.currentTimeMillis();
    if (cacheTime <= 0 || (now - lastUpdateTime > cacheTime)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("queueName: " + this.queueName + " ,now: " + now
            + " ,lastUpdateTime: " + lastUpdateTime + " ,over cacheTime: "
            + cacheTime + " ,start to reorder apps!");
      }
      reorderScheduleEntities();
      lastUpdateTime = now;
    }
    Iterator<S> iterator = schedulableEntities.iterator();
    AppSelector selector = sel.getAppSelector();
    if (selector == null) {
      return iterator;
    } else {
      Iterator<S> filteringIterator = new Iterator() {
        private S cached;
        private boolean hasCached;

        @Override
        public boolean hasNext() {
          if (hasCached) {
            return true;
          }
          while (iterator.hasNext()) {
            cached = iterator.next();
            if (selector.accept(cached)) {
              hasCached = true;
              return true;
            }
          }
          return false;
        }

        @Override
        public Object next() {
          if (hasCached) {
            hasCached = false;
            return cached;
          }
          if (!hasNext()) {
            throw new NoSuchElementException();
          }
          return next();
        }
      };
      return filteringIterator;
    }
  }

  @Override
  public void configure(Map<String, String> conf) {
    this.queueName = conf.get("queueName");
    if (conf.containsKey(APPS_ORDER_CACHE_TIME)) {
      this.cacheTime =
          Long.parseLong(conf.get(APPS_ORDER_CACHE_TIME));
    }
    if (conf.containsKey(APPS_FULL_REORDER_INTERVAL_SECOND)) {
      this.fullReorderIntervalSecond =
          Integer.parseInt(conf.get(APPS_FULL_REORDER_INTERVAL_SECOND));
    }
    this.nextFullOrderTime = System.currentTimeMillis() +
        (fullReorderIntervalSecond + random.nextInt(fullReorderIntervalSecond))
            * 1000;
    if (conf.containsKey(APP_HIGH_FLAG_PRIORITY)) {
      this.highFlagPriority =
          Double.parseDouble(conf.get(APP_HIGH_FLAG_PRIORITY));
    }
    if (conf.containsKey(APP_USED_FLAG_MEMORY)) {
      this.usedFlagMemory = Double.parseDouble(conf.get(APP_USED_FLAG_MEMORY));
    }
    if (conf.containsKey(APP_PENDING_FLAG_TIME)) {
      this.pendingFlagTime =
          Double.parseDouble(conf.get(APP_PENDING_FLAG_TIME));
    }
    if (conf.containsKey(APP_PRIORITY_WEIGHT_FACTOR)) {
      this.priorityWeightFactor =
          Double.parseDouble(conf.get(APP_PRIORITY_WEIGHT_FACTOR));
    }
    if (conf.containsKey(APP_USED_MEMORY_WEIGHT_FACTOR)) {
      this.usedMemoryWeightFactor =
          Double.parseDouble(conf.get(APP_USED_MEMORY_WEIGHT_FACTOR));
    }
    if (conf.containsKey(APP_TIME_WEIGHT_FACTOR)) {
      this.pendingTimeWeightFactor =
          Double.parseDouble(conf.get(APP_TIME_WEIGHT_FACTOR));
    }
    validateWeightFactor();
    LOG.info("highFlagPriority: " + highFlagPriority + " ,usedFlagMemory: "
        + usedFlagMemory + " ,pendingFlagTime: " + pendingFlagTime
        + " ,getAppOrderCacheTime: " + cacheTime
        + " ,fullReorderIntervalSecond: " + fullReorderIntervalSecond
        + " ,priorityWeightFactor: " + priorityWeightFactor
        + " ,usedMemoryWeightFactor: " + usedMemoryWeightFactor
        + " ,pendingTimeWeightFactor: " + pendingTimeWeightFactor);
  }

  private void validateWeightFactor() {
    if (priorityWeightFactor + usedMemoryWeightFactor + pendingTimeWeightFactor
        != 1) {
      LOG.warn("Invalid WeightFactor config, fall back to default config, "
          + "priorityWeightFactor: " + this.priorityWeightFactor
          + " ,usedMemoryWeightFactor: " + this.usedMemoryWeightFactor
          + " ,pendingTimeWeightFactor: " + pendingTimeWeightFactor);
      priorityWeightFactor = DEFAULT_APP_PRIORITY_WEIGHT_FACTOR;
      usedMemoryWeightFactor = DEFAULT_APP_USED_MEMORY_WEIGHT_FACTOR;
      pendingTimeWeightFactor = DEFAULT_APP_TIME_WEIGHT_FACTOR;
    }
  }

  @Override
  public void containerAllocated(S schedulableEntity, RMContainer r) {
    entityRequiresReordering(schedulableEntity);
  }

  @Override
  public void containerReleased(S schedulableEntity, RMContainer r) {
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
