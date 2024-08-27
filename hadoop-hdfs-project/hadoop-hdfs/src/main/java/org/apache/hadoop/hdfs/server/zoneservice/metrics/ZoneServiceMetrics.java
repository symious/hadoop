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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MutableRate;

import java.util.concurrent.ConcurrentHashMap;

@Metrics(name="ZoneServiceMetrics", about="ZoneService metrics", context="ZSinternal")
public class ZoneServiceMetrics {
  final MetricsRegistry registry = new MetricsRegistry("zoneservice");

  @Metric MutableGaugeInt monitorThreadCount;
  @Metric MutableGaugeInt batchThreadCount;
  @Metric MutableGaugeInt checkThreadCount;
  @Metric MutableRate checkRecordCostTime;
  @Metric MutableCounterLong successTotalMoveCount;
  @Metric MutableCounterLong failTotalMoveCount;
  // For ZoneMover monitor thread of a namespace
  private final ConcurrentHashMap<String, MutableCounterLong> nsMonitorSuccessMoveCount
      = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, MutableCounterLong> nsMonitorFailMoveCount
      = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, MutableCounterLong> nsBatchSuccessMoveCount
      = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, MutableCounterLong> nsBatchFailMoveCount
      = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, MutableCounterLong> nsCheckSuccessMoveCount
      = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, MutableCounterLong> nsCheckFailMoveCount
      = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, MutableRate> nsKafkaOffsetZk
      = new ConcurrentHashMap<>();

  public static ZoneServiceMetrics create() {
    return DefaultMetricsSystem.instance().register(new ZoneServiceMetrics());
  }

  public void startMonitorThread() { monitorThreadCount.incr(); }
  public void stopMonitorThread() { monitorThreadCount.decr(); }
  public void startBatchThread() { batchThreadCount.incr(); }
  public void stopBatchThread() { batchThreadCount.decr(); }
  public void startCheckThread() { checkThreadCount.incr(); }
  public void stopCheckThread() { checkThreadCount.decr(); }
  public void addCheckRecordCostTime(long costTime) { checkRecordCostTime.add(costTime); }
  public void incrSuccessMoveCount() { successTotalMoveCount.incr(); }
  public void incrFailMoveCount() { failTotalMoveCount.incr(); }

  public void incrNSMonitorSuccessMoveCount(String ns) {
    incrNSMoveCount(ns, nsMonitorSuccessMoveCount, "nsMonitorSuccessMoveCount");
  }

  public void incrNSMonitorFailMoveCount(String ns) {
    incrNSMoveCount(ns, nsMonitorFailMoveCount, "nsMonitorFailMoveCount");
  }

  public void incrNSBatchSuccessMoveCount(String ns) {
    incrNSMoveCount(ns, nsBatchSuccessMoveCount, "NSBatchSuccessMoveCount");
  }

  public void incrNSBatchFailMoveCount(String ns) {
    incrNSMoveCount(ns, nsBatchFailMoveCount, "NSBatchFailMoveCount");
  }

  public void incrNSCheckSuccessMoveCount(String ns) {
    incrNSMoveCount(ns, nsCheckSuccessMoveCount, "NSCheckSuccessMoveCount");
  }

  public void incrNSCheckFailMoveCount(String ns) {
    incrNSMoveCount(ns, nsCheckFailMoveCount, "NSCheckFailMoveCount");
  }

  public void incrNSMoveCount(String ns, ConcurrentHashMap<String, MutableCounterLong>
      counterLongMap, String name) {
    if (ns == null || counterLongMap == null) {
      return;
    }

    if (!counterLongMap.containsKey(ns)) {
      synchronized (this) {
        if (!counterLongMap.containsKey(ns)) {
          String metricName = StringUtils.capitalize(ns + name);
          counterLongMap.put(ns,
              registry.newCounter(Interns.info(metricName, metricName), 0L));
        }
      }
    }
    counterLongMap.get(ns).incr();
  }

  public void incrNSKafkaOffsetZk(String ns, String name, long duration) {
    if (ns == null) {
      return;
    }

    nsKafkaOffsetZk.computeIfAbsent(ns, key -> {
      String metricName = StringUtils.capitalize(ns + name);
      return this.registry.newRate(metricName);
    }).add(duration);
  }

  public MutableCounterLong getSuccessTotalMoveCount() {
    return successTotalMoveCount;
  }

  public void shutdown() { DefaultMetricsSystem.shutdown(); }
}