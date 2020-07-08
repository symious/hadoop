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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.metrics2.lib.Interns.*;


@InterfaceAudience.Private
@Metrics(context="yarn")
public class SchedulerEventDispatcherMetrics extends DispatcherMetrics {

  static final Logger LOG = LoggerFactory.getLogger(SchedulerEventDispatcherMetrics.class);
  static final MetricsInfo RECORD_INFO = info("SchedulerEventDispatcherMetrics",
      "Metrics for scheduler async dispatcher");

  static boolean initialized;

  @Metric("Node added event count") MutableCounterLong nodeAddedCount;
  @Metric("Node added processing time") MutableCounterLong nodeAddedTimeUs;
  @Metric("Node removed event count") MutableCounterLong nodeRemovedCount;
  @Metric("Node removed processing time") MutableCounterLong nodeRemovedTimeUs;
  @Metric("Node update event count") MutableCounterLong nodeUpdateCount;
  @Metric("Node update processing time") MutableCounterLong nodeUpdateTimeUs;
  @Metric("Node resource update event count") MutableCounterLong nodeResourceUpdateCount;
  @Metric("Node resource update processing time") MutableCounterLong nodeResourceUpdateTimeUs;
  @Metric("Node labels update event count") MutableCounterLong nodeLabelsUpdateCount;
  @Metric("Node labels update processing time") MutableCounterLong nodeLabelsUpdateTimeUs;
  @Metric("App added event count") MutableCounterLong appAddedCount;
  @Metric("App added processing time") MutableCounterLong appAddedTimeUs;
  @Metric("App removed event count") MutableCounterLong appRemovedCount;
  @Metric("App removed processing time") MutableCounterLong appRemovedTimeUs;
  @Metric("App attempt added event count") MutableCounterLong appAttemptAddedCount;
  @Metric("App attempt added processing time") MutableCounterLong appAttemptAddedTimeUs;
  @Metric("App attempt removed event count") MutableCounterLong appAttemptRemovedCount;
  @Metric("App attempt removed processing time") MutableCounterLong appAttemptRemovedTimeUs;
  @Metric("Container expired event count") MutableCounterLong containerExpiredCount;
  @Metric("Container expired processing time") MutableCounterLong containerExpiredTimeUs;

  protected SchedulerEventDispatcherMetrics(MetricsSystem ms) {
    super(ms, RECORD_INFO);
  }

  protected static StringBuilder sourceName() {
    return new StringBuilder(RECORD_INFO.name());
  }

  public synchronized
  static SchedulerEventDispatcherMetrics registerMetrics() {
    SchedulerEventDispatcherMetrics metrics = null;
    MetricsSystem ms = DefaultMetricsSystem.instance();
    if (!initialized) {
      // Register with the MetricsSystems
      if (ms != null) {
        metrics = new SchedulerEventDispatcherMetrics(ms);
        LOG.info("Registering SchedulerEventDispatcherMetrics");
        ms.register(
            sourceName().toString(),
            "Metrics for scheduler async dispatcher", metrics);
        initialized = true;
      }
    }

    return metrics;
  }

  public void incrementEventType(Event event, long processingTimeUs) {
    LOG.debug("Got scheduler event of type " + event.getType());
    switch ((SchedulerEventType) (event.getType())) {
      case NODE_ADDED:
        nodeAddedCount.incr();
        nodeAddedTimeUs.incr(processingTimeUs);
        break;
      case NODE_REMOVED:
        nodeRemovedCount.incr();
        nodeRemovedTimeUs.incr(processingTimeUs);
        break;
      case NODE_UPDATE:
        nodeUpdateCount.incr();
        nodeUpdateTimeUs.incr(processingTimeUs);
        break;
      case NODE_RESOURCE_UPDATE:
        nodeResourceUpdateCount.incr();
        nodeResourceUpdateTimeUs.incr(processingTimeUs);
        break;
      case NODE_LABELS_UPDATE:
        nodeLabelsUpdateCount.incr();
        nodeLabelsUpdateTimeUs.incr(processingTimeUs);
        break;
      case APP_ADDED:
        appAddedCount.incr();
        appAddedTimeUs.incr(processingTimeUs);
        break;
      case APP_REMOVED:
        appRemovedCount.incr();
        appRemovedTimeUs.incr(processingTimeUs);
        break;
      case APP_ATTEMPT_ADDED:
        appAttemptAddedCount.incr();
        appAttemptAddedTimeUs.incr(processingTimeUs);
        break;
      case APP_ATTEMPT_REMOVED:
        appAttemptRemovedCount.incr();
        appAttemptRemovedTimeUs.incr(processingTimeUs);
        break;
      case CONTAINER_EXPIRED:
        containerExpiredCount.incr();
        containerExpiredTimeUs.incr(processingTimeUs);
        break;
      default:
        break;
    }
  }
}
