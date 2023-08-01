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
import org.apache.hadoop.hdfs.server.zoneservice.utils.MigrationDataCenters;
import org.apache.hadoop.hdfs.server.zoneservice.utils.RunMode;
import org.apache.hadoop.hdfs.server.zoneservice.utils.ZoneServiceUtil;
import org.apache.hadoop.hdfs.server.zoneservice.web.resources.ResultCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Semaphore;

/**
 * This class runs ZoneMoverWithSetReplication in {@link RunMode#CHECK} mode to update
 * historical file replication rules, then remove the according {@link MigrationRecord}
 * from the state store.
 */
public class CheckThread extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(CheckThread.class);
  private final String path;
  private final String replicationRule;
  private final String nameSpace;
  private final Configuration conf;
  private final ZoneServiceMetrics metrics;
  private final StoreDriver driver;
  private final Semaphore semaphore;
  private final List<String> inProcessPaths;
  private final String clientIDC;

  public CheckThread(String path, String replicationRule, String ns, Configuration conf,
      StoreDriver driver, Semaphore semaphore, List<String> inProcessPaths, String clientIDC) {
    super("Zone-Check" + ns + "_" + path);
    this.path = path;
    this.replicationRule = replicationRule;
    this.nameSpace = ns;
    this.conf = conf;
    this.metrics = ZoneService.getMetrics();
    this.driver = driver;
    this.semaphore = semaphore;
    this.inProcessPaths = inProcessPaths;
    this.clientIDC = clientIDC;
  }

  @Override
  public void run() {
    Date startTime = new Date();
    try {
      LOG.info("Starting check move process for path: {}", path);
      metrics.startCheckThread();
      ResultCode resultCode = movePath(conf, nameSpace, path);

      AuditLogger.logRuleProcess("Check", nameSpace, path, replicationRule,
          startTime, new Date(), resultCode.getMsg(), RunMode.CHECK.getName());

      if (resultCode.equal(ResultCode.SUCCESS)) {
        metrics.incrNSCheckSuccessMoveCount(nameSpace);
        metrics.incrSuccessMoveCount();
      } else {
        metrics.incrNSCheckFailMoveCount(nameSpace);
        metrics.incrFailMoveCount();
      }

      // Catch ZK remove check record exception and record zk error log.
      try {
        LOG.info("Removing check record for path: {} in namespace: {}", path, nameSpace);
        MigrationRecord migrationRecord = new MigrationRecord(
            nameSpace, path, replicationRule, RunMode.CHECK.getName(), clientIDC);
        driver.remove(new Query<>(migrationRecord), MigrationRecord.class);
      } catch (IOException e) {
        AuditLogger.logRuleProcess(
            "Check", nameSpace, path, replicationRule,
            startTime, new Date(), ResultCode.IO_EXCEPTION.getMsg(), RunMode.CHECK.getName());
        LOG.error("Failed to remove check zk record for path: {}", path, e);
      }
    } catch (IOException e) {
      AuditLogger.logRuleProcess(
          "Check", nameSpace, path, replicationRule,
          startTime, new Date(), ResultCode.IO_EXCEPTION.getMsg(), RunMode.CHECK.getName());
      LOG.error("Check process for path: {} failed.", path, e);
    } catch (InterruptedException e) {
      AuditLogger.logRuleProcess(
          "Check", nameSpace, path, replicationRule,
          startTime, new Date(), ResultCode.INTERRUPTED.getMsg(), RunMode.CHECK.getName());
      LOG.warn("Check process for path: {} is interrupted.", path, e);
    } catch (Throwable e) {
      AuditLogger.logRuleProcess(
          "Check", nameSpace, path, replicationRule,
          startTime, new Date(), ResultCode.UNKNOWNERROR.getMsg(), RunMode.CHECK.getName());
      LOG.error("Check process for path: {} stopped by unknown error.", path, e);
    } finally {
      LOG.info("Check move process for path: {} is done.", path);
      metrics.stopCheckThread();
      inProcessPaths.remove(path);
      semaphore.release();
    }
  }

  private ResultCode movePath(Configuration conf, String nameSpace, String path)
      throws InterruptedException, IOException {
    final URI namenode = ZoneServiceUtil.getNamespaceUri(nameSpace, conf);
    final List<Path> paths = Collections.singletonList(new Path(path));
    return ZoneServiceUtil.convertExitStatus2ResultCode(ExitStatus.getExitStatusByCode(
        ZoneMoverWithSetReplication.checkWithSetReplication(
            conf, namenode, paths, MigrationDataCenters.fromName(clientIDC))));
  }
}
