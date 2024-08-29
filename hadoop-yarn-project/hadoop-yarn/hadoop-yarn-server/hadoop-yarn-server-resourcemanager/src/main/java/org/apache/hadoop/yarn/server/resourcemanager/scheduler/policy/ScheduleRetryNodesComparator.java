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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;

/**
 * A Comparator which orders SchedulableEntities by lastScheduleRetryNodes,
 * used to punish abnormal apps that retry too many nodes
 */
public class ScheduleRetryNodesComparator implements Comparator<SchedulableEntity> {

  private static final Logger LOG =
      LoggerFactory.getLogger(ScheduleRetryNodesComparator.class);

  private int maxRetryNodesThreshold = 500;

  public int getMaxRetryNodesThreshold() {
    return maxRetryNodesThreshold;
  }

  public void setMaxRetryNodesThreshold(int maxRetryNodesThreshold) {
    this.maxRetryNodesThreshold = maxRetryNodesThreshold;
  }

  @Override
  public int compare(SchedulableEntity se1, SchedulableEntity se2) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("maxRetryNodesThreshold: " + maxRetryNodesThreshold);
    }
    int lastScheduleRetryNodes1 = se1.getCachedScheduleRetryNodes();
    int lastScheduleRetryNodes2 = se2.getCachedScheduleRetryNodes();
    if (LOG.isDebugEnabled()) {
      LOG.debug("app1: " + se1.getId() + " ,lastScheduleRetryNodes1: " +
          lastScheduleRetryNodes1 + " ,app2: " + se2.getId() +
          " ,lastScheduleRetryNodes2: " + lastScheduleRetryNodes2);
    }
    if (lastScheduleRetryNodes1 <= this.maxRetryNodesThreshold &&
        lastScheduleRetryNodes2 <= this.maxRetryNodesThreshold) {
      return 0;
    } else {
      return Integer.compare(lastScheduleRetryNodes1, lastScheduleRetryNodes2);
    }
  }

}
