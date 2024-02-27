/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.policy;

import org.apache.commons.collections.iterators.IteratorChain;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractCSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * Traverse queues:
 * - Queues with low usage are placed first,
 *   and queues with high usage are placed later
 */
public class Utilization2RandomQueueOrderingPolicy
    implements QueueOrderingPolicy {

  private static final Logger LOG =
      LoggerFactory.getLogger(Utilization2RandomQueueOrderingPolicy.class);

  private List<CSQueue> queues;

  private final Random random = new Random();

  @Override
  public void setQueues(List<CSQueue> queues) {
    this.queues = queues;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Iterator<CSQueue> getAssignmentIterator(String partition,
      Set<String> otherLookupPartitions) {
    long start = System.nanoTime();

    //combine candidate nodePartition and crossPartitions
    List<String> candidateAllPartition = new ArrayList<>();
    if(otherLookupPartitions != null && otherLookupPartitions.size() > 0){
      candidateAllPartition.addAll(otherLookupPartitions);
    }
    candidateAllPartition.add(partition);

    CSQueue parentQueue = queues.get(0).getParent();
    String parentQueuePath =
        (parentQueue == null) ? "root" : parentQueue.getQueuePath();

    List<CSQueue> originalQueues = new ArrayList<>(queues);
    IteratorChain iteratorChain = new IteratorChain();

    try {
      //1. filter lowUtilizationQueues & highUtilizationQueues
      List<CSQueue> lowUtilizationQueues = new ArrayList<>();
      List<CSQueue> highUtilizationQueues = new ArrayList<>();

      for (CSQueue queue : originalQueues) {
        boolean isLowUtilizationQueue = false;
        for (String candidatePartition : candidateAllPartition) {
          if (queue instanceof AbstractCSQueue && !((AbstractCSQueue) queue)
              .accessibleToPartition(candidatePartition)) {
            continue;
          }
          double usedCapacity =
              queue.getQueueCapacities().getUsedCapacity(candidatePartition);
          Resource pendingResource =
              queue.getQueueResourceUsage().getPending(candidatePartition);
          long pendingMemoryMB = pendingResource.getMemorySize();
          int pendingVCores = pendingResource.getVirtualCores();
          if (usedCapacity < 1.0 &&
              (pendingMemoryMB > 0 || pendingVCores > 0)) {
            isLowUtilizationQueue = true;
            break;
          }
        }
        if (isLowUtilizationQueue) {
          lowUtilizationQueues.add(queue);
        } else {
          highUtilizationQueues.add(queue);
        }
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("lowUtilizationQueues: " + lowUtilizationQueues);
      }

      //2. random lowUtilizationQueues & random highUtilizationQueues -> orderAllQueues
      int lowSize = lowUtilizationQueues.size();
      if (lowSize > 0) {
        int index = random.nextInt(lowSize);
        iteratorChain.addIterator(
            lowUtilizationQueues.subList(index, lowSize).iterator());
        iteratorChain
            .addIterator(lowUtilizationQueues.subList(0, index).iterator());
      }

      int highSize = highUtilizationQueues.size();
      if (highSize > 0) {
        int index2 = random.nextInt(highSize);
        iteratorChain.addIterator(
            highUtilizationQueues.subList(index2, highSize).iterator());
        iteratorChain
            .addIterator(highUtilizationQueues.subList(0, index2).iterator());
      }

    } catch (Exception e) {
      //Exception fall to use random policy, should never happen
      List<CSQueue> randomOrderQueues = new ArrayList<>(queues);
      Collections.shuffle(randomOrderQueues);
      LOG.error("Get queue: [" + parentQueuePath +
          "] order child queues from cache failed!", e);
      return randomOrderQueues.iterator();
    }
    long end = System.nanoTime();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Utilization2RandomQueueOrderingPolicy getAssignmentIterator " +
          "cost time: " + (end - start) / 1000 + " us!");
    }
    return iteratorChain;
  }

  @Override
  public String getConfigName() {
    return CapacitySchedulerConfiguration.
        QUEUE_UTILIZATION_RANDOM_ORDERING_POLICY;
  }

  @VisibleForTesting
  public List<CSQueue> getQueues() {
    return queues;
  }
}
