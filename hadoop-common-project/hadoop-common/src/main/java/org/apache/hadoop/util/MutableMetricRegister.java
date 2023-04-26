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

package org.apache.hadoop.util;

import java.util.Map;

import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableMetric;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.metrics2.lib.MutableStat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MutableMetricRegister {
  public static final Logger LOG = LoggerFactory.getLogger(MutableMetricRegister.class);

  public static <T extends MutableMetric> T tryGetMetric(MetricsRegistry registry, String key,
      Map<String, T> metricMap, String metricPrefix, Class<T> metricClass) {
    if (!metricMap.containsKey(key)) {
      synchronized (metricMap) {
        if (!metricMap.containsKey(key)) {
          String metricName = StringUtils.capitalize(metricPrefix + key + "_");
          T metric;
          if (metricClass == MutableStat.class) {
            metric = (T) registry.newStat(metricName, metricName, "Ops", "Val", false);
          } else if (metricClass == MutableRate.class) {
            metric = (T) registry.newRate(metricName, metricName);
          } else if (metricClass == MutableCounterLong.class) {
            metric = (T) registry.newCounter(metricName, metricName, 0L);
          } else {
            LOG.warn("Class type {} not supported by {}", metricClass.getName(), registry.info());
            return null;
          }
          metricMap.put(key, metric);
        }
      }
    }
    return metricMap.get(key);
  }
}
