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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheLoader;
import org.apache.hadoop.thirdparty.com.google.common.cache.LoadingCache;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.FutureCallback;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.Futures;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Traverse queues:
 * - Queues with low usage are placed first,
 *   and queues with high usage or no access label are placed later
 */
public class Utilization2RandomQueueOrderingPolicy
    implements QueueOrderingPolicy {

  private static final Logger LOG =
      LoggerFactory.getLogger(Utilization2RandomQueueOrderingPolicy.class);

  private List<CSQueue> queues;

  private LoadingCache<String, List<List<CSQueue>>> cache;
  private long cacheTimeout;
  private boolean reloadQueuesInBackground;
  private int reloadQueuesThreadCount;
  private final AtomicLong backgroundRefreshSuccess =
      new AtomicLong(0);
  private final AtomicLong backgroundRefreshException =
      new AtomicLong(0);
  private final AtomicLong backgroundRefreshQueued =
      new AtomicLong(0);
  private final AtomicLong backgroundRefreshRunning =
      new AtomicLong(0);

  /**
   * Deals with loading data into the cache.
   */
  private class QueuesCacheLoader extends CacheLoader<String, List<List<CSQueue>>> {

    private ListeningExecutorService executorService;

    QueuesCacheLoader() {
      if (reloadQueuesInBackground) {
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
            .setNameFormat("Queues-Cache-Reload")
            .setDaemon(true)
            .build();
        // With coreThreadCount == maxThreadCount we effectively
        // create a fixed size thread pool. As allowCoreThreadTimeOut
        // has been set, all threads will die after 60 seconds of non use
        ThreadPoolExecutor parentExecutor = new ThreadPoolExecutor(
            reloadQueuesThreadCount,
            reloadQueuesThreadCount,
            60,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            threadFactory);
        parentExecutor.allowCoreThreadTimeOut(true);
        executorService = MoreExecutors.listeningDecorator(parentExecutor);
      }
    }

    @Override
    public List<List<CSQueue>> load(String key) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("loading key: " + key);
      }

      String partition = key.substring(key.indexOf(",") + 1);

      List<CSQueue> originalQueues = new ArrayList<>(queues);

      List<List<CSQueue>> allQueues = new ArrayList<>();
      List<CSQueue> lowUtilizationQueues = new ArrayList<>();
      List<CSQueue> highUtilizationQueues = new ArrayList<>();

      boolean accessible = false;
      if (StringUtils.equals(partition, RMNodeLabelsManager.NO_LABEL)) {
        accessible = true;
      }

      for (CSQueue queue : originalQueues) {
        boolean isQueueContainLabel =
            queue.getAccessibleNodeLabels().contains(partition);
        float usedCapacity =
            queue.getQueueCapacities().getUsedCapacity(partition);
        long pendingMemoryMB = queue.getMetrics().getPendingMB();
        int pendingVCores = queue.getMetrics().getPendingVirtualCores();
        if (usedCapacity < 1.0 && (accessible || isQueueContainLabel) &&
            (pendingMemoryMB > 0 || pendingVCores > 0)) {
          lowUtilizationQueues.add(queue);
        } else {
          highUtilizationQueues.add(queue);
        }
      }

      allQueues.add(lowUtilizationQueues);
      allQueues.add(highUtilizationQueues);

      if (LOG.isDebugEnabled()) {
        LOG.debug("After Utilization2RandomQueueOrderingPolicy allQueues: " +
            allQueues);
      }

      return allQueues;
    }

    /**
     * Override the reload method to provide an asynchronous implementation. If
     * reloadQueuesInBackground is false, then this method defers to the super
     * implementation, otherwise is arranges for the cache to be updated later
     */
    @Override
    public ListenableFuture<List<List<CSQueue>>> reload(final String key,
        List<List<CSQueue>> oldValue)
        throws Exception {
      LOG.debug("QueuesCacheLoader - reload (async).");
      if (!reloadQueuesInBackground) {
        return super.reload(key, oldValue);
      }

      backgroundRefreshQueued.incrementAndGet();
      ListenableFuture<List<List<CSQueue>>> listenableFuture =
          executorService.submit(new Callable<List<List<CSQueue>>>() {
            @Override
            public List<List<CSQueue>> call() throws Exception {
              backgroundRefreshQueued.decrementAndGet();
              backgroundRefreshRunning.incrementAndGet();
              List<List<CSQueue>> results = load(key);
              return results;
            }
          });
      Futures.addCallback(listenableFuture,
          new FutureCallback<List<List<CSQueue>>>() {
            @Override
            public void onSuccess(List<List<CSQueue>> result) {
              backgroundRefreshSuccess.incrementAndGet();
              backgroundRefreshRunning.decrementAndGet();
            }

            @Override
            public void onFailure(Throwable t) {
              backgroundRefreshException.incrementAndGet();
              backgroundRefreshRunning.decrementAndGet();
            }
          }, MoreExecutors.directExecutor());

      if (LOG.isDebugEnabled()) {
        LOG.debug("backgroundRefreshSuccess: " + backgroundRefreshSuccess +
            " ,backgroundRefreshRunning: " + backgroundRefreshRunning +
            " ,backgroundRefreshException: " + backgroundRefreshException +
            " ,backgroundRefreshQueued: " + backgroundRefreshQueued);
      }

      return listenableFuture;
    }

  }

  public Utilization2RandomQueueOrderingPolicy(boolean reloadQueuesInBackground,
      int reloadQueuesThreadCount, long cacheTime) {

    this.reloadQueuesInBackground = reloadQueuesInBackground;
    this.reloadQueuesThreadCount = reloadQueuesThreadCount;
    this.cacheTimeout = cacheTime;

    this.cache = CacheBuilder.newBuilder()
        .refreshAfterWrite(cacheTimeout, TimeUnit.MILLISECONDS)
        .build(new QueuesCacheLoader());
  }

  @Override
  public void setQueues(List<CSQueue> queues) {
    this.queues = queues;
  }

  @Override
  public Iterator<CSQueue> getAssignmentIterator(String partition) {
    long start = System.nanoTime();
    CSQueue parentQueue = queues.get(0).getParent();
    String parentQueuePath =
        (parentQueue == null) ? "root" : parentQueue.getQueuePath();
    List<CSQueue> orderAllQueues = new ArrayList<>();

    try {
      String key = parentQueuePath + "," + partition;
      List<List<CSQueue>> allQueues = cache.get(key);
      List<CSQueue> lowUtilizationQueues = allQueues.get(0);
      List<CSQueue> highUtilizationQueues = allQueues.get(1);

      //shuffle cache result
      List<CSQueue> lowUtilizationQueues_shuffle = new ArrayList<>();
      lowUtilizationQueues_shuffle.addAll(lowUtilizationQueues);
      Collections.shuffle(lowUtilizationQueues_shuffle);

      List<CSQueue> highUtilizationQueues_shuffle = new ArrayList<>();
      highUtilizationQueues_shuffle.addAll(highUtilizationQueues);
      Collections.shuffle(highUtilizationQueues_shuffle);

      orderAllQueues.addAll(lowUtilizationQueues_shuffle);
      orderAllQueues.addAll(highUtilizationQueues_shuffle);

    } catch (Exception e) {
      //Exception fall to use random policy, should never happen
      orderAllQueues = new ArrayList<>(queues);
      Collections.shuffle(orderAllQueues);
      LOG.error("Get queue: [" + parentQueuePath +
          "] order child queues from cache failed!", e);
    }
    long end = System.nanoTime();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Utilization2RandomQueueOrderingPolicy getAssignmentIterator " +
          "cost time: " + (end - start) / 1000 + " us!");
    }
    return orderAllQueues.iterator();
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
