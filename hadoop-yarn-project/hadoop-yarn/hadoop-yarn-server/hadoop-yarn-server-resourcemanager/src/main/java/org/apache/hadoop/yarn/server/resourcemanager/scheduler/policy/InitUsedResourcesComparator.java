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

import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;

/**
 * A Comparator which orders SchedulableEntities by init used resources,
 * to avoid AM container of low weight apps can't start
 */
public class InitUsedResourcesComparator implements Comparator<SchedulableEntity> {

  private static final Logger LOG =
      LoggerFactory.getLogger(InitUsedResourcesComparator.class);

  @Override
  public int compare(SchedulableEntity r1, SchedulableEntity r2) {
    long r1_used_resources = r1.getSchedulingResourceUsage().getCachedUsed(
        CommonNodeLabelsManager.ANY).getMemorySize();
    long r2_used_resources = r2.getSchedulingResourceUsage().getCachedUsed(
        CommonNodeLabelsManager.ANY).getMemorySize();
    if (r1_used_resources == 0 && r2_used_resources > 0) {
      LOG.debug(
          "appId: " + r1.getId() + " ,used resources: " + r1_used_resources);
      return -1;
    } else if (r1_used_resources > 0 && r2_used_resources == 0) {
      LOG.debug(
          "appId: " + r2.getId() + " ,used resources: " + r2_used_resources);
      return 1;
    } else {
      return 0;
    }

  }
}
