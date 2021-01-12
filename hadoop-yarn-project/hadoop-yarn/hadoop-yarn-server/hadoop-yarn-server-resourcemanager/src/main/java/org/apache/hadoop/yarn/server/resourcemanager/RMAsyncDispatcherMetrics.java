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

package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.yarn.event.DispatcherMetrics;
import org.apache.hadoop.yarn.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.metrics2.lib.Interns.*;

@InterfaceAudience.Private
@Metrics(context="yarn")
public class RMAsyncDispatcherMetrics extends DispatcherMetrics {

  static final Logger LOG = LoggerFactory.getLogger(RMAsyncDispatcherMetrics.class);
  static final MetricsInfo RECORD_INFO = info("RMAsyncDispatcherMetrics",
      "Metrics for RM async dispatcher");

  static boolean initialized;

  @Metric("Node usable event count") MutableCounterLong nodeUsableCount;
  @Metric("Node usable processing time") MutableCounterLong nodeUsableTimeUs;
  @Metric("Node unusable event count") MutableCounterLong nodeUnusableCount;
  @Metric("Node unusable processing time") MutableCounterLong nodeUnusableTimeUs;

  protected RMAsyncDispatcherMetrics(MetricsSystem ms) {
    super(ms, RECORD_INFO);
  }

  protected static StringBuilder sourceName() {
    return new StringBuilder(RECORD_INFO.name());
  }

  public synchronized
  static RMAsyncDispatcherMetrics registerMetrics() {
    RMAsyncDispatcherMetrics metrics = null;
    MetricsSystem ms = DefaultMetricsSystem.instance();
    if (!initialized) {
      // Register with the MetricsSystems
      if (ms != null) {
        metrics = new RMAsyncDispatcherMetrics(ms);
        LOG.info("Registering RMAsyncDispatcherMetrics");
        ms.register(
            sourceName().toString(),
            "Metrics for RM async dispatcher", metrics);
        initialized = true;
      }
    }

    return metrics;
  }

  public void incrementEventType(Event event, long processingTimeUs) {
    if (!(event.getType() instanceof NodesListManagerEventType)) {
      return;
    }
    switch ((NodesListManagerEventType) (event.getType())) {
      case NODE_USABLE:
        nodeUsableCount.incr();
        nodeUsableTimeUs.incr(processingTimeUs);
        break;
      case NODE_UNUSABLE:
        nodeUnusableCount.incr();
        nodeUnusableTimeUs.incr(processingTimeUs);
        break;
    }
  }
}
