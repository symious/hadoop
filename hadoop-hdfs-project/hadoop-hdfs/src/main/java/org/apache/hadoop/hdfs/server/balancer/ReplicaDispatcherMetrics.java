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
package org.apache.hadoop.hdfs.server.balancer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.metrics2.lib.MutableRate;

@Metrics(name="ReplicaDispatcherMetrics", about="Replica Dispatcher metrics",
    context="ReplicaDispatcher")
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ReplicaDispatcherMetrics {
  private final MetricsRegistry registry;
  private final String name;

  // The number of blocks successfully scheduled.
  @Metric("The rate of scheduled blocks")
  MutableCounterLong successScheduledBlocks;
  // The number of blocks that failed to be scheduled.
  @Metric("The rate of fail schedule blocks")
  MutableCounterLong failedScheduledBlocks;
  // The number of tasks running
  @Metric("The number of running tasks")
  MutableGaugeLong runningTasks;
  // The number of pending tasks
  @Metric("The number of pending tasks")
  MutableGaugeLong pendingTasks;
  // The total size that be moved.
  @Metric("The total size that moved")
  MutableCounterLong movedBytes;
  // The average time and qps of chooseTarget
  @Metric("The rate of choosing target")
  MutableRate chooseTarget;
  // The average time and qps of chooseProxy
  @Metric("The rate of choosing proxy")
  MutableRate chooseProxy;
  // The time and qps of successful blocks
  MutableQuantiles successBlocksQuantiles;
  // Task waiting time
  @Metric("Queue time")
  MutableRate taskQueueTime;
  // Time of successful task
  @Metric("Rate of success blocks")
  MutableRate successBlocks;
  // The number of successful blocks
  @Metric("Number of total success migrated blocks")
  MutableCounterLong successMigrateBlocks;
  // The number of failed blocks
  @Metric("Number of failed blocks")
  MutableCounterLong failedBlocks;
  // The number of blocks that succeeded after retry.
  @Metric("Number of success blocks after retry")
  MutableCounterLong successBlocksAfterRetry;

  public ReplicaDispatcherMetrics() {
    this.name = "ReplicaDispatcherMetrics";
    this.registry = new MetricsRegistry(this.name);
    this.successBlocksQuantiles = this.registry.newQuantiles("successBlocks60s",
        "success blocks in second", "ops", "latency", 60);
  }

  public static ReplicaDispatcherMetrics create() {
    ReplicaDispatcherMetrics replicaDispatcherMetrics =
        new ReplicaDispatcherMetrics();
    return DefaultMetricsSystem.instance().register(replicaDispatcherMetrics);
  }

  public void incrScheduledBlocks() {
    successScheduledBlocks.incr();
  }

  public void incrFailedScheduledBlocks() {
    failedScheduledBlocks.incr();
  }


  public void incrRunningTasks() {
    runningTasks.incr();
  }

  public void decrRunningTasks() {
    runningTasks.decr();
  }

  public void incrPendingTasks() {
    pendingTasks.incr();
  }

  public void decrPendingTasks() {
    pendingTasks.decr();
  }

  public void addChooseTarget(long duration) {
    this.chooseTarget.add(duration);
  }

  public void addChooseProxy(long duration) {
    this.chooseProxy.add(duration);
  }

  public void addSuccessBlocks(long duration) {
    successBlocksQuantiles.add(duration);
    successBlocks.add(duration);
    successMigrateBlocks.incr();
  }

  public void addTaskQueueTime(long duration) {
    taskQueueTime.add(duration);
  }

  public void incrFailedBlocks() {
    failedBlocks.incr();
  }

  public void incrSuccessBlocksAfterRetry() {
    successBlocksAfterRetry.incr();
  }

  public void incrMovedBytes(long bytes) {
    this.movedBytes.incr(bytes);
  }
}
