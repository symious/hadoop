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
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.balancer.ExitStatus;
import org.apache.hadoop.hdfs.server.zoneservice.store.MigrationRecord;
import org.apache.hadoop.hdfs.server.zoneservice.store.Query;
import org.apache.hadoop.hdfs.server.zoneservice.store.SignalRecord;
import org.apache.hadoop.hdfs.server.zoneservice.store.StoreDriver;
import org.apache.hadoop.hdfs.server.zoneservice.web.resources.ResultCode;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Semaphore;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ZONESERVICE_STORE_DRIVER_CLASS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ZONESERVICE_STORE_DRIVER_CLASS_DEFAULT;

/**
 * The class manages the operations related to replication rules.
 */
public class ReplicationRuleManager {
  private static final Logger LOG = LoggerFactory.getLogger(
      ReplicationRuleManager.class);

  private final Set<String> validDataCenters;
  private final Semaphore semaphore;
  private final StoreDriver driver;
  private final static String SECTION_SEPARATOR = ",";

  public ReplicationRuleManager(Configuration conf) {
    String datacenters = conf.get(
        DFSConfigKeys.DFS_ZONEMOVER_VALID_DATACENTERS_KEY,
        DFSConfigKeys.DFS_ZONEMOVER_VALID_DATACENTERS_DEFAULT);
    validDataCenters = new HashSet<>
        (Arrays.asList(datacenters.trim().split(SECTION_SEPARATOR)));

    int maxThread = conf.getInt(
        DFSConfigKeys.DFS_ZONESERVICE_THREADS_KEY,
        DFSConfigKeys.DFS_ZONESERVICE_THREADS_DEFAULT);
    semaphore = new Semaphore(maxThread);

    Class<? extends StoreDriver> driverClass = conf.getClass(
        DFS_ZONESERVICE_STORE_DRIVER_CLASS,
        DFS_ZONESERVICE_STORE_DRIVER_CLASS_DEFAULT,
        StoreDriver.class);
    driver =
        ReflectionUtils.newInstance(driverClass, conf);
    driver.init(conf, "ReplicationRuleManager");
  }

  /**
   * check path with zone checker
   * @param nameSpace URI of the NameNode
   * @param path      the path to be checked
   * @param ratio     the ratio of path will be checked
   * @return check result: replication-path map
   */
  public Map<ReplicationRule, Set<String>> checkPath(String nameSpace,
      String path, String ratio) {
    Configuration conf = new Configuration();
    URI namenode = getNamespaceUri(nameSpace, conf);
    return ZoneChecker.getReplicaRule(conf, namenode, path,
        Float.parseFloat(ratio));
  }

  /**
   * set batch mode process and handle the exceptions
   * @param nameSpace    URI of the NameNode
   * @param replicaRule  the replica rule to apply
   * @param path         the path to apply the rule
   * @return status of result
   */
  public ResultCode setBatchProcess(String nameSpace, String path,
      String replicaRule) {
    //Check rule valid for input
    if (checkRuleInvalid(validDataCenters, replicaRule)) {
      return ResultCode.ILLEGAL_ARGUMENTS;
    }

    Date startTime = new Date();
    String currentMethod =
        Thread.currentThread().getStackTrace()[1].getMethodName();
    try {
      MigrationRecord migrationRecord =
          new MigrationRecord(nameSpace, path, replicaRule);
      //If there is a rule applying on the path, reject the query
      if (driver.get(new Query<>(migrationRecord), MigrationRecord.class)
          != null) {
        AuditLogger.logRuleProcess(currentMethod, nameSpace,
            path, replicaRule, startTime, new Date(),
            ResultCode.REJECT.getMsg(), "batch");
        return ResultCode.REJECT;
      }

      //Check if there is any available thread
      if (semaphore.availablePermits() == 0) {
        AuditLogger.logRuleProcess(currentMethod, nameSpace,
            path, replicaRule, startTime, new Date(),
            ResultCode.THREAD_FULL.getMsg(), "batch");
        return ResultCode.THREAD_FULL;
      }

      semaphore.acquire();
      driver.put(migrationRecord, false, true);
      ResultCode result = movePath(nameSpace, path, replicaRule);
      semaphore.release();
      driver.remove(new Query<>(migrationRecord), MigrationRecord.class);
      AuditLogger.logRuleProcess(currentMethod, nameSpace, path, replicaRule,
          startTime, new Date(), result.getMsg(), "batch");
      return result;
    } catch (InterruptedException e) {
      LOG.error("Batch process on {} is interrupted.", path, e);
      AuditLogger.logRuleProcess(currentMethod, nameSpace,
          path, replicaRule, startTime, new Date(),
          ResultCode.INTERRUPTED.getMsg(), "batch");
      return ResultCode.INTERRUPTED;
    } catch (IOException e) {
      LOG.error("Fail to apply the rule {} on {}.", replicaRule, path, e);
      AuditLogger.logRuleProcess(currentMethod, nameSpace,
          path, replicaRule, startTime, new Date(),
          ResultCode.IO_EXCEPTION.getMsg(), "batch");
      return ResultCode.IO_EXCEPTION;
    }
  }

  /**
   * Create or update path rule map in zk and let ZoneMover process know
   * @param nameSpace    name of the namespace
   * @param path         the path will be updated
   * @param replicaRule  the rule will be applied
   * @param allowCreate  allow create new map by this method or not
   * @return the status of the result
   */
  public ResultCode createUpdateMap(String nameSpace, String path,
      String replicaRule, boolean allowCreate) {
    //Check rule valid for input
    if (checkRuleInvalid(validDataCenters, replicaRule)) {
      return ResultCode.ILLEGAL_ARGUMENTS;
    }

    Date startTime = new Date();
    try {
      String threadName = "monitor_" + nameSpace;
      SignalRecord signalRecord = new SignalRecord(nameSpace, true);
      MigrationRecord migrationRecord = new MigrationRecord(nameSpace, path,
          replicaRule, "monitor");
      MigrationRecord existedRecord =
          driver.get(new Query<>(migrationRecord), MigrationRecord.class);
      if (existedRecord != null) {
        //If there is batch mode running on the path, reject the query
        if (existedRecord.getMode().equals("batch")) {
          AuditLogger.logRuleProcess(
              "updatePathRuleMap", nameSpace,
              path, replicaRule, startTime, new Date(),
              ResultCode.REJECT.getMsg(), "monitor");
          return ResultCode.REJECT;
        }
        // If it is the same rule, return.
        if (checkRuleEquals(existedRecord.getRule(), replicaRule)) {
          LOG.info("The {} already has the replicationRule {}.", path, replicaRule);
          return ResultCode.CREATE_SUCCESS;
        }
      }
      Thread[] ts = new Thread[Thread.activeCount()];
      Thread.enumerate(ts);
      for (Thread tt : ts) {
        //If the thread is existed the new path-rule will add into the thread
        if (tt.getName().equals(threadName)) {
          if (existedRecord == null) {
            driver.put(migrationRecord, true, false);
            driver.put(signalRecord, true, false);
            AuditLogger.logRuleProcess(
                "setPathRuleMap", nameSpace,
                path, replicaRule, startTime, new Date(),
                ResultCode.CREATE_SUCCESS.getMsg(), "monitor");
            return ResultCode.CREATE_SUCCESS;
          }
        }
      }
      if (!allowCreate) {
        //no thread on this namespace, not allow to create thread by this method
        AuditLogger.logRuleProcess(
            "refreshPathRuleMap", nameSpace,
            path, replicaRule, startTime, new Date(),
            ResultCode.METHOD_ERROR.getMsg(), "monitor");
        return ResultCode.METHOD_ERROR;
      }
      //If there is no monitor thread for this namespace, it will create a new one
      Thread monitorThread = new MonitorThread(threadName, new Configuration(),
          getNamespaceUri(nameSpace, new Configuration()));
      driver.put(migrationRecord, true, true);
      driver.put(signalRecord, true, false);
      monitorThread.start();
      AuditLogger.logRuleProcess(
          "CreatePathRuleMap", nameSpace,
          path, replicaRule, startTime, new Date(),
          ResultCode.CREATE_SUCCESS.getMsg(), "monitor");
      return ResultCode.CREATE_SUCCESS;
    } catch (IOException e) {
      LOG.error("Failed {} to createUpdateMap the replicationRule {}.",path, replicaRule, e);
      AuditLogger.logRuleProcess(
          "CreatePathRuleMap", nameSpace,
          path, replicaRule, startTime, new Date(),
          ResultCode.IO_EXCEPTION.getMsg(), "monitor");
      return ResultCode.IO_EXCEPTION;
    }
  }

  /**
   * remove the path-rule map from zk by the given path
   * @param nameSpace name of the namespace
   * @param path      the path whose path-rule pair will be removed
   * @return the status of result
   */
  public ResultCode removePathRulePair(String nameSpace, String path) {
    Date startTime = new Date();
    try {
      MigrationRecord migrationRecord = new MigrationRecord(nameSpace, path,
          "");
      SignalRecord signalRecord = new SignalRecord(nameSpace, true);
      if (driver.get(new Query<>(migrationRecord),
          MigrationRecord.class) == null) {
        AuditLogger.logRuleProcess(
            "DeletePathRuleMap", nameSpace,
            path, "N/A", startTime, new Date(),
            ResultCode.NO_MIGRATION_RECORD.getMsg(), "monitor");
        return ResultCode.NO_MIGRATION_RECORD;
      } else {
        driver.remove(new Query<>(migrationRecord), MigrationRecord.class);
        driver.put(signalRecord, true, false);
        AuditLogger.logRuleProcess(
            "DeletePathRuleMap", nameSpace,
            path, "N/A", startTime, new Date(),
            ResultCode.SUCCESS.getMsg(), "monitor");
        return ResultCode.SUCCESS;
      }
    } catch (IOException e) {
      LOG.error("Fail to remove the rule on {}.", path, e);
      AuditLogger.logRuleProcess(
          "DeletePathRuleMap", nameSpace,
          path, "N/A", startTime, new Date(),
          ResultCode.IO_EXCEPTION.getMsg(), "monitor");
      return ResultCode.IO_EXCEPTION;
    }
  }

  /**
   * Move the path with ZoneMover
   * @param nameSpace   URI of the NameNode
   * @param path        the path to apply the rule
   * @param replicaRule the replica rule to apply
   * @return ResultCode
   */
  private ResultCode movePath(String nameSpace, String path,
      String replicaRule) throws InterruptedException, IOException {
    final Configuration conf = new Configuration();
    final URI namenode = getNamespaceUri(nameSpace, conf);
    final ReplicationRule replicationRule =
        ReplicationRule.parseFromString(replicaRule);
    final List<Path> paths = new ArrayList<>();
    paths.add(new org.apache.hadoop.fs.Path(path));

    switch (Objects.requireNonNull(ExitStatus.getExitStatusByCode(
        ZoneMover.run(conf, namenode, paths, replicationRule)))) {
      case SUCCESS:
        return ResultCode.SUCCESS;
      case IN_PROGRESS:
        return ResultCode.IN_PROGRESS;
      case ALREADY_RUNNING:
        return ResultCode.ALREADY_RUNNING;
      case NO_MOVE_BLOCK:
        return ResultCode.NO_MOVE_BLOCK;
      case NO_MOVE_PROGRESS:
        return ResultCode.NO_MOVE_PROGRESS;
      case IO_EXCEPTION:
        return ResultCode.IO_EXCEPTION;
      case ILLEGAL_ARGUMENTS:
        return ResultCode.ILLEGAL_ARGUMENTS;
      case INTERRUPTED:
        return ResultCode.INTERRUPTED;
      case UNFINALIZED_UPGRADE:
        return ResultCode.UNFINALIZED_UPGRADE;
    }
    return ResultCode.UNKNOWNERROR;
  }

  private URI getNamespaceUri(String namespace, Configuration conf)
      throws IllegalArgumentException {
    Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
    for (URI namenode: namenodes) {
      if (namenode.getAuthority().equals(namespace)) {
        return namenode;
      }
    }
    throw new IllegalArgumentException(
        "Cannot find the NameNode for namespace: " + namespace);
  }

  private boolean checkRuleInvalid(Set<String> validDataCenters, String rule) {
    try {
      //Check replica valid
      ReplicationRule replicationRule = ReplicationRule.parseFromString(rule);

      //Check DC valid
      for (ReplicationRuleSection section: replicationRule.getSections()) {
        if (!validDataCenters.contains(section.getDataCenter())) {
          return true;
        }
      }
    } catch (IllegalArgumentException e) {
      return true;
    }
    return false;
  }

  private boolean checkRuleEquals(String oldRule, String newRule) {
    try {
      return ReplicationRule.parseFromString(oldRule).equals(
          ReplicationRule.parseFromString(newRule));
    } catch (IllegalArgumentException e) {
      return false;
    }
  }
}
