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

package org.apache.hadoop.metrics2.lib;

import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;

import static org.apache.hadoop.metrics2.lib.Interns.info;

/**
 * {@link MutableRollingAverages} but has rolling average rate of ingested
 * entries per second for each metric.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MutableRollingAveragesWithRateAndMax
    extends MutableRollingAverages {

  private final String rollingRateNameTemplate;
  private final String rollingRateDescTemplate;
  private final String rollingMaxNameTemplate;
  private final String rollingMaxDescTemplate;

  /**
   * Constructor for {@link MutableRollingAveragesWithRateAndMax}.
   * @param metricValueName
   */
  public MutableRollingAveragesWithRateAndMax(String metricValueName,
      MutableRatesWithAggregation externalMetrics) {
    super(metricValueName);
    if (metricValueName == null) {
      metricValueName = "";
    }
    if (externalMetrics != null) {
      innerMetrics = externalMetrics;
    }

    rollingRateNameTemplate = "[%s]" + "Rate" +
        StringUtils.capitalize(metricValueName);
    rollingRateDescTemplate = "Average number of collected metrics " +
        StringUtils.uncapitalize(metricValueName) +" per sec for "+ "%s";
    rollingMaxNameTemplate = "[%s]" + "Max" +
        StringUtils.capitalize(metricValueName);
    rollingMaxDescTemplate = "Max " +
        StringUtils.uncapitalize(metricValueName) +" last rolling window for "+ "%s";
  }

  @Override
  public void snapshot(MetricsRecordBuilder builder, boolean all) {
    if (all || changed()) {
      for (final Entry<String, LinkedBlockingDeque<SumAndMaxAndCount>> entry
          : averages.entrySet()) {
        final String name = entry.getKey();
        final MetricsInfo rollingAvg = info(
            String.format(avgInfoNameTemplate, StringUtils.capitalize(name)),
            String.format(avgInfoDescTemplate, StringUtils.uncapitalize(name)));
        final MetricsInfo rollingMax = info(
            String.format(rollingMaxNameTemplate, StringUtils.capitalize(name)),
            String.format(rollingMaxDescTemplate, StringUtils.uncapitalize(name)));
        final MetricsInfo rollingRate = info(
            String.format(rollingRateNameTemplate, StringUtils.capitalize(name)),
            String.format(rollingRateDescTemplate, StringUtils.uncapitalize(name)));
        double totalSum = 0;
        double totalMax = 0;
        long totalCount = 0;
        int windowCounted = 0;

        for (final SumAndMaxAndCount sumAndMaxAndCount : entry.getValue()) {
          totalCount += sumAndMaxAndCount.getCount();
          totalMax = Math.max(totalMax, sumAndMaxAndCount.getMax());
          totalSum += sumAndMaxAndCount.getSum();
          windowCounted++;
        }

        if (totalCount != 0) {
          builder.addGauge(rollingAvg, totalSum / totalCount);
          builder.addGauge(rollingMax, totalMax);
          builder.addGauge(rollingRate,
              (double) totalCount / windowCounted / recordValidityMs * 1000);
        }
      }
      if (changed()) {
        clearChanged();
      }
    }
  }
}
