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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Metrics(context="yarn")
public class FSQueueMetrics extends QueueMetrics {

  @Metric("Fair share of memory in MB") MutableGaugeLong fairShareMB;
  @Metric("Fair share of CPU in vcores") MutableGaugeLong fairShareVCores;
  @Metric("Steady fair share of memory in MB") MutableGaugeLong steadyFairShareMB;
  @Metric("Steady fair share of CPU in vcores") MutableGaugeLong steadyFairShareVCores;
  @Metric("Minimum share of memory in MB") MutableGaugeLong minShareMB;
  @Metric("Minimum share of CPU in vcores") MutableGaugeLong minShareVCores;
  @Metric("Maximum share of memory in MB") MutableGaugeLong maxShareMB;
  @Metric("Maximum share of CPU in vcores") MutableGaugeLong maxShareVCores;
  @Metric("Maximum number of applications") MutableGaugeInt maxApps;
  @Metric("Maximum AM share of memory in MB") MutableGaugeLong maxAMShareMB;
  @Metric("Maximum AM share of CPU in vcores") MutableGaugeInt maxAMShareVCores;
  @Metric("AM resource usage of memory in MB") MutableGaugeLong amResourceUsageMB;
  @Metric("AM resource usage of CPU in vcores") MutableGaugeInt amResourceUsageVCores;

  @Metric("Sort time of FSParent.assign.sort of queue in nano seconds") MutableGaugeLong fsParentAssignSortNS;
  @Metric("Max Sort time of FSParent.assign.sort of queue in nano seconds") MutableGaugeLong maxFsParentAssignSortNS;
  @Metric("Min Sort time of FSParent.assign.sort of queue in nano seconds") MutableGaugeLong minFsParentAssignSortNS;

  @Metric("Assign time of FSLeaf.assign of queue in nano seconds") MutableGaugeLong fsLeafAssignNS;
  @Metric("Max Assign time of FSLeaf.assign of queue in nano seconds") MutableGaugeLong maxFsLeafAssignNS;
  @Metric("Min Assign time of FSLeaf.assign of queue in nano seconds") MutableGaugeLong minFsLeafAssignNS;

  @Metric("Assign time of FSAppAttempt of queue in nano seconds") MutableGaugeLong fsAppAttemptAssignNS;
  @Metric("Max Allocated time of queue in nano seconds") MutableGaugeLong maxFsAppAttemptAssignNS;
  @Metric("Min Allocated time of queue in nano seconds") MutableGaugeLong minFsAppAttemptAssignNS;

  private String schedulingPolicy;

  private final SchedulerMetric schedulerMetric = new SchedulerMetric();

  FSQueueMetrics(MetricsSystem ms, String queueName, Queue parent,
      boolean enableUserMetrics, Configuration conf) {
    super(ms, queueName, parent, enableUserMetrics, conf);
  }

  public void setFairShare(Resource resource) {
    fairShareMB.set(resource.getMemorySize());
    fairShareVCores.set(resource.getVirtualCores());
  }

  public long getFairShareMB() {
    return fairShareMB.value();
  }

  public long getFairShareVirtualCores() {
    return fairShareVCores.value();
  }

  public void setSteadyFairShare(Resource resource) {
    steadyFairShareMB.set(resource.getMemorySize());
    steadyFairShareVCores.set(resource.getVirtualCores());
  }

  public long getSteadyFairShareMB() {
    return steadyFairShareMB.value();
  }

  public long getSteadyFairShareVCores() {
    return steadyFairShareVCores.value();
  }

  public void setMinShare(Resource resource) {
    minShareMB.set(resource.getMemorySize());
    minShareVCores.set(resource.getVirtualCores());
  }

  public long getMinShareMB() {
    return minShareMB.value();
  }

  public long getMinShareVirtualCores() {
    return minShareVCores.value();
  }

  public void setMaxShare(Resource resource) {
    maxShareMB.set(resource.getMemorySize());
    maxShareVCores.set(resource.getVirtualCores());
  }

  public long getMaxShareMB() {
    return maxShareMB.value();
  }

  public long getMaxShareVirtualCores() {
    return maxShareVCores.value();
  }

  public int getMaxApps() {
    return maxApps.value();
  }

  public void setMaxApps(int max) {
    maxApps.set(max);
  }

  /**
   * Get the maximum memory size AM can use in MB.
   *
   * @return the maximum memory size AM can use
   */
  public long getMaxAMShareMB() {
    return maxAMShareMB.value();
  }

  /**
   * Get the maximum number of VCores AM can use.
   *
   * @return the maximum number of VCores AM can use
   */
  public int getMaxAMShareVCores() {
    return maxAMShareVCores.value();
  }

  /**
   * Set the maximum resource AM can use.
   *
   * @param resource the maximum resource AM can use
   */
  public void setMaxAMShare(Resource resource) {
    maxAMShareMB.set(resource.getMemorySize());
    maxAMShareVCores.set(resource.getVirtualCores());
  }

  /**
   * Get the AM memory usage in MB.
   *
   * @return the AM memory usage
   */
  public long getAMResourceUsageMB() {
    return amResourceUsageMB.value();
  }

  /**
   * Get the AM VCore usage.
   *
   * @return the AM VCore usage
   */
  public int getAMResourceUsageVCores() {
    return amResourceUsageVCores.value();
  }

  /**
   * Set the AM resource usage.
   *
   * @param resource the AM resource usage
   */
  public void setAMResourceUsage(Resource resource) {
    amResourceUsageMB.set(resource.getMemorySize());
    amResourceUsageVCores.set(resource.getVirtualCores());
  }

  public long getFsParentAssignSortNS() {
    return fsParentAssignSortNS.value();
  }

  public void setFsParentAssignSortNS(long fsParentAssignSortNS) {
    this.fsParentAssignSortNS.set(fsParentAssignSortNS);
  }

  public long getMaxFsParentAssignSortNS() {
    return maxFsParentAssignSortNS.value();
  }

  public void setMaxFsParentAssignSortNS(long maxFsParentAssignSortNS) {
    this.maxFsParentAssignSortNS.set(maxFsParentAssignSortNS);
  }

  public long getMinFsParentAssignSortNS() {
    return minFsParentAssignSortNS.value();
  }

  public void setMinFsParentAssignSortNS(long minFsParentAssignSortNS) {
    this.minFsParentAssignSortNS.set(minFsParentAssignSortNS);
  }

  public long getFsLeafAssignNS() {
    return fsLeafAssignNS.value();
  }

  public void setFsLeafAssignNS(long fsLeafAssignNS) {
    this.fsLeafAssignNS.set(fsLeafAssignNS);
  }

  public long getMaxFsLeafAssignNS() {
    return maxFsLeafAssignNS.value();
  }

  public void setMaxFsLeafAssignNS(long maxFsLeafAssignNS) {
    this.maxFsLeafAssignNS.set(maxFsLeafAssignNS);
  }

  public long getMinFsLeafAssignNS() {
    return minFsLeafAssignNS.value();
  }

  public void setMinFsLeafAssignNS(long minFsLeafAssignNS) {
    this.minFsLeafAssignNS.set(minFsLeafAssignNS);
  }


  public long getFsAppAttemptAssignNS() {
    return fsAppAttemptAssignNS.value();
  }

  public void setFsAppAttemptAssignNS(long fsAppAttemptAssignNS) {
    this.fsAppAttemptAssignNS.set(fsAppAttemptAssignNS);
  }

  public long getMaxFsAppAttemptAssignNS() {
    return maxFsAppAttemptAssignNS.value();
  }

  public void setMaxFsAppAttemptAssignNS(long maxFsAppAttemptAssignNS) {
    this.maxFsAppAttemptAssignNS.set(maxFsAppAttemptAssignNS);
  }

  public long getMinFsAppAttemptAssignNS() {
    return minFsAppAttemptAssignNS.value();
  }

  public void setMinFsAppAttemptAssignNS(long minFsAppAttemptAssignNS) {
    this.minFsAppAttemptAssignNS.set(minFsAppAttemptAssignNS);
  }

  /**
   * Get the scheduling policy.
   *
   * @return the scheduling policy
   */
  @Metric("Scheduling policy")
  public String getSchedulingPolicy() {
    return schedulingPolicy;
  }

  public void setSchedulingPolicy(String policy) {
    schedulingPolicy = policy;
  }

  public synchronized
  static FSQueueMetrics forQueue(String queueName, Queue parent,
      boolean enableUserMetrics, Configuration conf) {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return forQueue(ms, queueName, parent, enableUserMetrics, conf);
  }


  @Override
  public void allocateResources(String partition, String user,
      int containers, Resource res, boolean decrPending) {
    allocateResourcesUpdate(partition, user, containers, res, decrPending);
  }

  @Override
  public void allocateResources(String partition, String user, Resource res) {
    allocateResourcesUpdate(partition, user, res);
  }

  @Override
  public void releaseResources(String partition,
      String user, int containers, Resource res) {
    releaseResourcesUpdate(partition, user, containers, res);
  }

  @Override
  public void incrPendingResources(String partition, String user,
      int containers, Resource res) {
    incrPendingResourcesUpdate(partition, user, containers, res);
  }

  @Override
  public void decrPendingResources(String partition, String user,
      int containers, Resource res) {
    decrPendingResourcesUpdate(partition, user, containers, res);
  }


  /**
   * Get the FS queue metric for the given queue. Create one and register it to
   * metrics system if there isn't one for the queue.
   *
   * @param ms the metric system
   * @param queueName queue name
   * @param parent parent queue
   * @param enableUserMetrics  if user metrics is needed
   * @param conf configuration
   * @return an FSQueueMetrics object
   */
  @VisibleForTesting
  public synchronized
  static FSQueueMetrics forQueue(MetricsSystem ms, String queueName,
      Queue parent, boolean enableUserMetrics, Configuration conf) {
    QueueMetrics metrics = QueueMetrics.getQueueMetrics().get(queueName);
    if (metrics == null) {
      metrics = new FSQueueMetrics(ms, queueName, parent, enableUserMetrics, conf)
          .tag(QUEUE_INFO, queueName);

      // Register with the MetricsSystems
      if (ms != null) {
        metrics = ms.register(
            sourceName(queueName).toString(),
            "Metrics for queue: " + queueName, metrics);
      }
      QueueMetrics.getQueueMetrics().put(queueName, metrics);
    }

    return (FSQueueMetrics)metrics;
  }

  public static final String FS_PARENT_ASSIGN_SORT = "FsParentAssignSortNS";

  public static final String FS_LEAF_ASSIGN = "FsLeafAssignNS";

  public static final String FS_APP_ATTEMPT_ASSIGN = "FsAppAttemptAssignNS";

  private static final String MAX_FS_PARENT_ASSIGN_SORT = "Max" + FS_PARENT_ASSIGN_SORT;
  private static final String MIN_FS_PARENT_ASSIGN_SORT = "Min" + FS_PARENT_ASSIGN_SORT;
  private static final String LAST_FS_PARENT_ASSIGN_SORT = "Last" + FS_PARENT_ASSIGN_SORT;

  private static final String MAX_FS_LEAF_ASSIGN = "Max" + FS_LEAF_ASSIGN;
  private static final String MIN_FS_LEAF_ASSIGN = "Min" + FS_LEAF_ASSIGN;
  private static final String LAST_FS_LEAF_ASSIGN = "Last" + FS_LEAF_ASSIGN;


  private static final String MAX_FS_APP_ATTEMPT_ASSIGN = "Max" + FS_APP_ATTEMPT_ASSIGN;
  private static final String MIN_FS_APP_ATTEMPT_ASSIGN = "Min" + FS_APP_ATTEMPT_ASSIGN;
  private static final String LAST_FS_APP_ATTEMPT_ASSIGN = "Last" + FS_APP_ATTEMPT_ASSIGN;



  private long lastMs = Time.monotonicNow();

  private static final int INTERVAL = 60000;

  public void monitorSchedulerMetrics(String type, long value){
    schedulerMetric.monitor(type, value);
    updateSchedulerMetrics(type);
  }

  public void updateSchedulerMetrics(String type) {
    if (Time.monotonicNow() - lastMs > INTERVAL) {
      switch (type) {
        case FS_PARENT_ASSIGN_SORT:
          this.setMaxFsParentAssignSortNS(schedulerMetric.metricMap.get(MAX_FS_PARENT_ASSIGN_SORT));
          this.setMinFsParentAssignSortNS(schedulerMetric.metricMap.get(MIN_FS_PARENT_ASSIGN_SORT));
          this.setFsParentAssignSortNS(schedulerMetric.metricMap.get(LAST_FS_PARENT_ASSIGN_SORT));
          break;
        case FS_LEAF_ASSIGN:
          this.setMaxFsLeafAssignNS(schedulerMetric.metricMap.get(MAX_FS_LEAF_ASSIGN));
          this.setMinFsLeafAssignNS(schedulerMetric.metricMap.get(MIN_FS_LEAF_ASSIGN));
          this.setFsLeafAssignNS(schedulerMetric.metricMap.get(LAST_FS_LEAF_ASSIGN));
          break;
        case FS_APP_ATTEMPT_ASSIGN:
          this.setMaxFsAppAttemptAssignNS(schedulerMetric.metricMap.get(MAX_FS_APP_ATTEMPT_ASSIGN));
          this.setMinFsAppAttemptAssignNS(schedulerMetric.metricMap.get(MIN_FS_APP_ATTEMPT_ASSIGN));
          this.setFsAppAttemptAssignNS(schedulerMetric.metricMap.get(LAST_FS_APP_ATTEMPT_ASSIGN));
          break;
        default:
          break;
      }
      schedulerMetric.clear();
      lastMs = Time.monotonicNow();
    }
  }

  static class SchedulerMetric {

    private static final long DEFAULT_VALUE = 0;

    private final Map<String, Long> metricMap = new ConcurrentHashMap<>();

    public SchedulerMetric() {
      init();
    }

    public void monitor(String type, long value) {
      switch (type) {
        case FS_PARENT_ASSIGN_SORT:
          if (metricMap.get(MAX_FS_PARENT_ASSIGN_SORT) < value) {
            metricMap.put(MAX_FS_PARENT_ASSIGN_SORT, value);
          }
          if (metricMap.get(MIN_FS_PARENT_ASSIGN_SORT) > value ||
              metricMap.get(MIN_FS_PARENT_ASSIGN_SORT) == DEFAULT_VALUE) {
            metricMap.put(MIN_FS_PARENT_ASSIGN_SORT, value);
          }
          metricMap.put(LAST_FS_PARENT_ASSIGN_SORT, value);
          break;
        case FS_LEAF_ASSIGN:
          if (metricMap.get(MAX_FS_LEAF_ASSIGN) < value) {
            metricMap.put(MAX_FS_LEAF_ASSIGN, value);
          }
          if (metricMap.get(MIN_FS_LEAF_ASSIGN) > value ||
              metricMap.get(MIN_FS_LEAF_ASSIGN) == DEFAULT_VALUE) {
            metricMap.put(MIN_FS_LEAF_ASSIGN, value);
          }
          metricMap.put(LAST_FS_LEAF_ASSIGN, value);
          break;
        case FS_APP_ATTEMPT_ASSIGN:
          if (metricMap.get(MAX_FS_APP_ATTEMPT_ASSIGN) < value) {
            metricMap.put(MAX_FS_APP_ATTEMPT_ASSIGN, value);
          }
          if (metricMap.get(MIN_FS_APP_ATTEMPT_ASSIGN) > value ||
              metricMap.get(MIN_FS_APP_ATTEMPT_ASSIGN) == DEFAULT_VALUE) {
            metricMap.put(MIN_FS_APP_ATTEMPT_ASSIGN, value);
          }
          metricMap.put(LAST_FS_APP_ATTEMPT_ASSIGN, value);
          break;
        default:
          break;
      }
    }

    private void init() {
      initMetricMap();
    }

    private void clear() {
      initMetricMap();
    }

    //Set Metric as Default Status
    private void initMetricMap() {
      metricMap.put(MAX_FS_PARENT_ASSIGN_SORT, DEFAULT_VALUE);
      metricMap.put(MIN_FS_PARENT_ASSIGN_SORT, DEFAULT_VALUE);
      metricMap.put(LAST_FS_PARENT_ASSIGN_SORT, DEFAULT_VALUE);

      metricMap.put(MAX_FS_LEAF_ASSIGN, DEFAULT_VALUE);
      metricMap.put(MIN_FS_LEAF_ASSIGN, DEFAULT_VALUE);
      metricMap.put(LAST_FS_LEAF_ASSIGN, DEFAULT_VALUE);

      metricMap.put(MAX_FS_APP_ATTEMPT_ASSIGN, DEFAULT_VALUE);
      metricMap.put(MIN_FS_APP_ATTEMPT_ASSIGN, DEFAULT_VALUE);
      metricMap.put(LAST_FS_APP_ATTEMPT_ASSIGN, DEFAULT_VALUE);
    }

  }
}
