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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.balancer.ExitStatus;
import org.apache.hadoop.hdfs.server.zoneservice.metrics.ZoneServiceMetrics;
import org.apache.hadoop.hdfs.server.zoneservice.store.MigrationRecord;
import org.apache.hadoop.hdfs.server.zoneservice.store.Query;
import org.apache.hadoop.hdfs.server.zoneservice.store.StoreDriver;
import org.apache.hadoop.hdfs.server.zoneservice.utils.ZoneServiceUtil;
import org.apache.hadoop.hdfs.server.zoneservice.web.resources.ResultCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Semaphore;

public class BatchThread extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(BatchThread.class);
  private final String path;
  private final String replicationRule;
  private final String nameSpace;
  private final Configuration conf;
  private final ZoneServiceMetrics metrics;
  private final StoreDriver driver;
  private final Semaphore semaphore;
  private final List<String> inProcessPaths;

  public BatchThread(String path, String replicationRule, String ns,
      Configuration conf, StoreDriver driver, Semaphore semaphore, List<String> inProcessPaths) {
    super("batch_" + ns + "_" + path);
    this.path = path;
    this.replicationRule = replicationRule;
    this.nameSpace = ns;
    this.conf = conf;
    this.metrics = ZoneService.getMetrics();
    this.driver = driver;
    this.semaphore = semaphore;
    this.inProcessPaths = inProcessPaths;
  }

  @Override
  public void run() {
    Date startTime = new Date();
    // Check move process exception
    try {
      LOG.info("Batch move process for {} has been started.", this.path);
      metrics.startBatchThread();
      ResultCode resultCode = movePath(conf, nameSpace, path, replicationRule);

      AuditLogger.logRuleProcess(
          "Batch", nameSpace, path, replicationRule, startTime,
          new Date(), resultCode.getMsg(), "batch");

      if (resultCode.equal(ResultCode.SUCCESS)) {
        metrics.incrNSBatchSuccessMoveCount(nameSpace);
        metrics.incrSuccessMoveCount();
      } else {
        metrics.incrNSBatchFailMoveCount(nameSpace);
        metrics.incrFailMoveCount();
      }

      // Catch ZK remove record exception and record zk error log
      try {
        LOG.info("Remove batch record for {} in {}.", path, nameSpace);
        MigrationRecord migrationRecord = new MigrationRecord(
            nameSpace, path, replicationRule);
        driver.remove(new Query<>(migrationRecord), MigrationRecord.class);
      } catch (IOException e) {
        AuditLogger.logRuleProcess(
            "Batch", nameSpace, path, replicationRule,
            startTime, new Date(), ResultCode.IO_EXCEPTION.getMsg(), "batch");
        LOG.error("Remove zk record for {} fail.", path, e);
      }
    } catch (IOException e) {
      AuditLogger.logRuleProcess(
          "Batch", nameSpace, path, replicationRule,
          startTime, new Date(), ResultCode.IO_EXCEPTION.getMsg(), "batch");
      LOG.error("Batch process for {} failed.", path, e);
    } catch (InterruptedException e) {
      AuditLogger.logRuleProcess(
          "Batch", nameSpace, path, replicationRule,
          startTime, new Date(), ResultCode.INTERRUPTED.getMsg(), "batch");
      LOG.warn("Batch process for {} is interrupted.", path, e);
    } catch (Throwable e) {
      AuditLogger.logRuleProcess(
          "Batch", nameSpace, path, replicationRule,
          startTime, new Date(), ResultCode.UNKNOWNERROR.getMsg(), "batch");
      LOG.error("Batch process for {} is stopped by unknown error.", path, e);
    } finally {
      LOG.info("Batch move process for {} has been done.", this.path);
      metrics.stopBatchThread();
      inProcessPaths.remove(path);
      semaphore.release();
    }
  }

  private ResultCode movePath(Configuration conf, String nameSpace, String path,
      String replicaRule) throws InterruptedException, IOException {
    final URI namenode = ZoneServiceUtil.getNamespaceUri(nameSpace, conf);
    final ReplicationRule replicationRule =
        ReplicationRule.parseFromString(replicaRule);
    final List<Path> paths = new ArrayList<>();
    paths.add(new org.apache.hadoop.fs.Path(path));

    return ZoneServiceUtil.convertExitStatus2ResultCode(ExitStatus.getExitStatusByCode(
        ZoneMover.run(conf, namenode, paths, replicationRule)));
  }
}
