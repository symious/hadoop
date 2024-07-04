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

package org.apache.hadoop.hdfs.server.zoneservice;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.zoneservice.metrics.ZoneMoverMetrics;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;

public abstract class ZoneMoverTrigger {
  /**
   * After check, if we should trigger ZoneMover for another round
   * @return true if we need trigger ZoneMover,
   *         false if we need to stop ZoneMover current thread
   */
  public abstract boolean hasNext();

  /**
   * Get next elements from trigger
   * @return String contain the content trigger need feed back to ZoneMover
   */
  public abstract String getNext() throws InterruptedException;

  /**
   * Get next kafka record from trigger
   * @return String contain the content trigger need feed back to ZoneMover
   */
  public abstract Pair<ConsumerRecord<String, String>, String> getNextRecord()
      throws InterruptedException;

  /**
   * Update the care paths
   * @param paths the new path list
   */
  public abstract void updatePaths(List<Path> paths);
  /**
   * Close the thread and recycle the resources
   */
  public abstract void shutdown();

  public abstract String getGroupId();

  public abstract void saveOffsetToZookeeper(ConsumerRecord<String, String> record, String ns,
      String groupId, ZoneMoverMetrics zoneMoverMetrics);
}