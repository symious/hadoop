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
import org.apache.hadoop.metrics2.lib.MutableRate;

@Metrics(name="ZoneMoverMetrics", about="ZoneMover metrics", context="zm")
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ZoneMoverMetrics {

  final MetricsRegistry registry = new MetricsRegistry("ZoneMoverMetrics");

  @Metric("Rate of successful move and duration (ms)")
  MutableRate successTotalMove;

  @Metric("Rate of failed move and duration (ms)")
  MutableRate failTotalMove;

  @Metric("Rate and duration (ms) of recording Kafka offset in Zookeeper")
  MutableRate kafkaOffsetZk;

  public static ZoneMoverMetrics create() {
    return DefaultMetricsSystem.instance().register(new ZoneMoverMetrics());
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
}