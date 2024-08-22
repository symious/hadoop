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

package org.apache.hadoop.hdfs.server.zoneservice.metrics;

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

@Metrics(name="ZoneMoverMetrics", about="ZoneMover metrics", context="zm")
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ZoneMoverMetrics {

  private final MetricsRegistry registry;
  private final String name;

  @Metric("Rate of successful move and duration (ms)")
  MutableRate successTotalMove;

  @Metric("Rate of failed move and duration (ms)")
  MutableRate failTotalMove;

  @Metric("Rate and duration (ms) of recording Kafka offset in Zookeeper")
  MutableRate kafkaOffsetZk;

  // Time for successfully scheduled files
  @Metric("The rate of scheduled files")
  MutableRate scheduledFiles;
  // The time of files that failed to be scheduled.
  @Metric("The rate of fail schedule files")
  MutableRate failedScheduledFiles;
  // The number of pre-migration files
  @Metric("The number of pre-migration files")
  MutableCounterLong preMigrationFiles;
  // The number of pre-migration blocks to be processed
  @Metric("The number of pending pre-migration")
  MutableGaugeLong pendingPreMigration;
  // The number of files for which replication factor need to be changed.
  @Metric("The number of replication changed files")
  MutableCounterLong replicationChangedFiles;
  // The time of SetReplication operation.
  @Metric("The rate of change replication")
  MutableRate setReplication;
  // Time to add files to coordinator.
  @Metric("The rate of adding file of coordinator ")
  MutableRate addFileInCoordinator;
  // Total time to change and wait for replication factor changes
  @Metric("The rate of changing replication (total time)")
  MutableRate changeReplicationTotalTime;
  // The number of files whose replication factor is still changing
  @Metric("The number of total files that wait for replication to be ready")
  MutableGaugeLong waitingReplicationFiles;
  // The number of files whose replication factor is ready.
  @Metric("The number of total files that replication is ready")
  MutableGaugeLong replicationReadyFiles;

  MutableQuantiles successFileQuantiles;
  @Metric("Rate of success files")
  MutableRate successFiles;
  @Metric("Number of failed files")
  MutableCounterLong failedFiles;

  public ZoneMoverMetrics() {
    this.name = "ZoneMoverMetrics";
    this.registry = new MetricsRegistry(this.name);
    this.successFileQuantiles = this.registry.newQuantiles("successFiles60s",
        "success files in secod", "ops", "latency", 60);
  }

  public static ZoneMoverMetrics create() {
    ZoneMoverMetrics zoneMoverMetrics = new ZoneMoverMetrics();
    return DefaultMetricsSystem.instance().register(zoneMoverMetrics);
  }

  public void addSuccessTotalMove(long duration) {
    successTotalMove.add(duration);
  }

  public void addFailTotalMove(long duration) {
    failTotalMove.add(duration);
  }

  public void addKafkaOffsetZk(long duration) {
    kafkaOffsetZk.add(duration);
  }

  public MutableRate getSuccessTotalMove() {
    return successTotalMove;
  }

  public MutableRate getFailTotalMove() {
    return failTotalMove;
  }

  public MutableRate getKafkaOffsetZk() {
    return kafkaOffsetZk;
  }

  public void shutdown() {
    DefaultMetricsSystem.shutdown();
  }

  public void addScheduledFiles(long duration) {
    scheduledFiles.add(duration);
  }

  public void addFailedScheduledFiles(long duration) {
    failedScheduledFiles.add(duration);
  }

  public void incrPreMigrationFiles() {
    preMigrationFiles.incr();
  }

  public void incrPendingPreMigration() {
    pendingPreMigration.incr();
  }

  public void decrPendingPreMigration() {
    pendingPreMigration.decr();
  }

  public void incrReplicationChangedFiles() {
    replicationChangedFiles.incr();
  }

  public void addSetReplication(long duration) {
    setReplication.add(duration);
  }

  public void addFileInCoordinator(long duration) {
    addFileInCoordinator.add(duration);
  }

  public void addChangeReplicationTotalTime(long duration) {
    changeReplicationTotalTime.add(duration);
  }

  public void incrWaitingReplicationFiles() {
    waitingReplicationFiles.incr();
  }

  public void decrWaitingReplicationFiles() {
    waitingReplicationFiles.decr();
  }

  public void incrReplicationReadyFiles() {
    replicationReadyFiles.incr();
  }

  public void decrReplicationReadyFiles() {
    replicationReadyFiles.decr();
  }

  public void addSuccessFiles(long duration) {
    successFileQuantiles.add(duration);
    successFiles.add(duration);
  }

  public void incrFailedFiles() {
    failedFiles.incr();
  }
}