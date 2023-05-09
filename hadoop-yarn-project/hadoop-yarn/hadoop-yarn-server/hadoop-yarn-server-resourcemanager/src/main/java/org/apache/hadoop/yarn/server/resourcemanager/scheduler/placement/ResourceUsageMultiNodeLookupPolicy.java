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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * <p>
 * This class has the following functionality:
 *
 * <p>
 * ResourceUsageMultiNodeLookupPolicy holds sorted nodes list based on the
 * resource usage of nodes at given time.
 * </p>
 */
public class ResourceUsageMultiNodeLookupPolicy<N extends SchedulerNode>
    implements MultiNodeLookupPolicy<N> {

  private static final Logger LOG = LoggerFactory
      .getLogger(ResourceUsageMultiNodeLookupPolicy.class);

  protected Map<String, Set<N>> nodesPerPartition = new ConcurrentHashMap<>();
  protected Comparator<N> comparator;

  private int memoryResourcesUnit;

  public ResourceUsageMultiNodeLookupPolicy() {

    this.comparator = new Comparator<N>() {
      @Override
      public int compare(N o1, N o2) {

        Resource o1UnallocatedResource = o1.getUnallocatedResource();
        int o1UnallocatedVCores = o1UnallocatedResource.getVirtualCores();
        long o1UnallocatedMemoryUnit =
            o1UnallocatedResource.getMemorySize() / 1024 / memoryResourcesUnit;
        long o1CompareChooseResources =
            (o1UnallocatedVCores > o1UnallocatedMemoryUnit) ?
                o1UnallocatedMemoryUnit : o1UnallocatedVCores;

        Resource o2UnallocatedResource = o2.getUnallocatedResource();
        int o2UnallocatedVCores = o2UnallocatedResource.getVirtualCores();
        long o2UnallocatedMemoryUnit =
            o2UnallocatedResource.getMemorySize() / 1024 / memoryResourcesUnit;
        long o2CompareChooseResources =
            (o2UnallocatedVCores > o2UnallocatedMemoryUnit) ?
                o2UnallocatedMemoryUnit : o2UnallocatedVCores;

        int unAllocatedDiff =
            Long.compare(o2CompareChooseResources, o1CompareChooseResources);

        if (LOG.isDebugEnabled()) {
          LOG.debug("memoryResourcesUnit: " + memoryResourcesUnit + " ,o1Id:" +
              o1.getNodeID() + " ,o1UnallocatedVCores: " +
              o1UnallocatedVCores +
              " ,o1UnallocatedMemoryUnit: " + o1UnallocatedMemoryUnit +
              " ,o1CompareChooseResources: " + o1CompareChooseResources +
              " ,o2Id:" +
              o2.getNodeID() +
              " ,o2UnallocatedVCores: " + o2UnallocatedVCores +
              " ,o2UnallocatedMemoryUnit: " + o2UnallocatedMemoryUnit +
              " ,o2CompareChooseResources: " + o2CompareChooseResources +
              " ,unAllocatedDiff:" +
              unAllocatedDiff);
        }

        if (unAllocatedDiff == 0) {
          return o1.getNodeID().compareTo(o2.getNodeID());
        }
        return unAllocatedDiff;
      }
    };
  }

  @Override
  public Iterator<N> getPreferredNodeIterator(Collection<N> nodes,
      String partition) {
    return getNodesPerPartition(partition).iterator();
  }

  @Override
  public Iterator<N> getPreferredTopRandomNodeIterator(Collection<N> nodes,
      String partition, long skipNodeInterval, float topRate) {

    long start = System.nanoTime();
    List<N> allNodesList = new ArrayList<>();
    List<N> topNodesList = new ArrayList<>();

    Set<N> nodesPerPartitionSet = getNodesPerPartition(partition);
    Iterator<N> nodesPerPartitionIterator = nodesPerPartitionSet.iterator();
    int sumSize = nodesPerPartitionSet.size();
    int topRandomSize = Math.round(sumSize * topRate);

    int i = 0;
    while (nodesPerPartitionIterator.hasNext()) {
      N node = nodesPerPartitionIterator.next();
        if (i < topRandomSize) {
          topNodesList.add(node);
          i++;
        } else {
          break;
        }
    }

    if (topNodesList.size() == 0) {
      allNodesList.addAll(nodesPerPartitionSet);
    } else {
      Collections.shuffle(topNodesList);
      allNodesList.addAll(topNodesList);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "candidateNodes sumSize: " + sumSize + ", expectTopRandomSize: " +
              topRandomSize + " ,realTopRandomSize: " + allNodesList.size() +
              " ,getPreferredTopRandomNodeIterator cost time: " +
              (System.nanoTime() - start) / 1000 + " us.");
    }
    return allNodesList.iterator();
  }

  @Override
  public void addAndRefreshNodesSet(Collection<N> nodes,
      String partition) {
    long start = System.nanoTime();
    Set<N> nodeList = new ConcurrentSkipListSet<N>(comparator);
    nodeList.addAll(nodes);
    Set<N> putNodeSet = Collections.unmodifiableSet(nodeList);
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Partition: " + partition + ",sort nodes size:  " +
              putNodeSet.size() + " ,addAndRefreshNodesSet cost time: " +
              (System.nanoTime() - start) / 1000 + " us!");
    }
    nodesPerPartition.put(partition, putNodeSet);
  }

  @Override
  public Set<N> getNodesPerPartition(String partition) {
    return nodesPerPartition.getOrDefault(partition, Collections.emptySet());
  }

  @Override
  public void setMemoryResourcesUnit(int memoryResourcesUnit) {
    this.memoryResourcesUnit = memoryResourcesUnit;
  }
}
