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
import org.apache.hadoop.hdfs.server.zoneservice.store.MigrationRecord;
import org.apache.hadoop.hdfs.server.zoneservice.store.Query;
import org.apache.hadoop.hdfs.server.zoneservice.store.SignalRecord;
import org.apache.hadoop.hdfs.server.zoneservice.store.StoreDriver;
import org.apache.hadoop.hdfs.server.zoneservice.utils.MigrationDataCenters;
import org.apache.hadoop.hdfs.server.zoneservice.utils.ZoneServiceUtil;
import org.apache.hadoop.hdfs.server.zoneservice.web.resources.ResultCode;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ZONESERVICE_STORE_DRIVER_CLASS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ZONESERVICE_STORE_DRIVER_CLASS_DEFAULT;

/**
 * The class manages the operations related to replication rules.
 */
public class ReplicationRuleManager {
  private static final Logger LOG = LoggerFactory.getLogger(
      ReplicationRuleManager.class);

  private final Set<String> validDataCenters;
  private final StoreDriver driver;
  private final static String SECTION_SEPARATOR = ",";
  private final static String BATCH_MODE = "batch";
  private final static String MONITOR_MODE = "monitor";

  public ReplicationRuleManager(Configuration conf) {
    String datacenters = conf.get(
        DFSConfigKeys.DFS_ZONEMOVER_VALID_DATACENTERS_KEY,
        DFSConfigKeys.DFS_ZONEMOVER_VALID_DATACENTERS_DEFAULT);
    validDataCenters = new HashSet<>
        (Arrays.asList(datacenters.trim().split(SECTION_SEPARATOR)));

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
  public Map<ReplicationRule, Set<String>> checkPath(
      String nameSpace, String path, String ratio) {
    Configuration conf = new Configuration();
    URI namenode = ZoneServiceUtil.getNamespaceUri(nameSpace, conf);
    return ZoneChecker.getReplicaRule(conf, namenode, path,
        Float.parseFloat(ratio));
  }

  /**
   * get block summary of given path by data center
   * @param nameSpace URI of the NameNode
   * @param path      the path to be checked
   * @return block summary of the given path
   */
  public Map<String, List<Long>> summaryBlocks(String nameSpace, String path) {
    Configuration conf = new Configuration();
    URI namenode = ZoneServiceUtil.getNamespaceUri(nameSpace, conf);
    return ZoneChecker.getBlockSummary(conf, namenode, path);
  }

  public Map<String, List<Long>> countBlocksByDistribution(String nameSpace,
      String path) {
    Configuration conf = new Configuration();
    URI namenode = ZoneServiceUtil.getNamespaceUri(nameSpace, conf);
    return ZoneChecker.getCountSummary(conf, namenode, path);
  }

  /**
   * set batch mode process and handle the exceptions
   * @param nameSpace    URI of the NameNode
   * @param replicaRule  the replica rule to apply
   * @param path         the path to apply the rule
   * @return status of result
   */
  public ResultCode setBatchProcess(String nameSpace, String path, String replicaRule) {
    //Check rule valid for input
    if (checkRuleInvalid(validDataCenters, replicaRule)) {
      return ResultCode.ILLEGAL_ARGUMENTS;
    }

    Date startTime = new Date();
    String currentMethod =
        Thread.currentThread().getStackTrace()[1].getMethodName();
    try {
      MigrationRecord migrationRecord = new MigrationRecord(nameSpace, path, replicaRule);
      if (!driver.put(migrationRecord, false, true)) {
        AuditLogger.logRuleProcess(currentMethod, nameSpace,
            path, replicaRule, startTime, new Date(),
            ResultCode.REJECT.getMsg(), BATCH_MODE);
        return ResultCode.REJECT;
      }
      return ResultCode.IN_PROGRESS;
    } catch (IOException e) {
      LOG.error("Fail to put the rule into ZooKeeper.", e);
      AuditLogger.logRuleProcess(currentMethod, nameSpace,
          path, replicaRule, startTime, new Date(),
          ResultCode.IO_EXCEPTION.getMsg(), BATCH_MODE);
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
    final String methodName = "createUpdateMap";
    try {
      String threadName = "monitor_" + nameSpace;
      SignalRecord signalRecord = new SignalRecord(nameSpace, true);
      SignalRecord existedSignalRecord =
          driver.get(new Query<>(signalRecord), SignalRecord.class);

      if (existedSignalRecord == null) {
        if (!allowCreate) {
          // There is no monitor for this namespace and will not create it
          // because allowCreate is false.
          LOG.warn("Monitor thread for {} not exist and not allow to create.", nameSpace);
          AuditLogger.logRuleProcess(methodName, nameSpace,
              path, replicaRule, startTime, new Date(),
              ResultCode.METHOD_ERROR.getMsg(), MONITOR_MODE);
          return ResultCode.METHOD_ERROR;
        }

        // If there is no monitor thread for this namespace, it will create a new one
        LOG.info("Monitor thread for {} not exist and need to create.", nameSpace);
        createMonitorThread(nameSpace, threadName, driver, signalRecord, path, replicaRule,
            startTime);
      }
      MigrationRecord migrationRecordBatch = new MigrationRecord(nameSpace, path, replicaRule);
      MigrationRecord migrationRecord = new MigrationRecord(nameSpace, path,
          replicaRule, MONITOR_MODE);
      MigrationRecord existedRecord =
          driver.get(new Query<>(migrationRecord), MigrationRecord.class);
      // Check monitor record
      ResultCode resultCode = ResultCode.CREATE_SUCCESS;
      if (existedRecord != null) {
        // If it is the same rule, return.
        if (checkRuleEquals(existedRecord.getRule(), replicaRule)) {
          LOG.info("The {} already has the replicationRule {}.", path, replicaRule);
          return ResultCode.REJECT;
        } else {
          LOG.info("The {} has the different replicationRule, old: {} and new: {}.",
              path, existedRecord.getRule(), replicaRule);
          resultCode = ResultCode.UPDATE_SUCCESS;
        }
      }
      driver.put(migrationRecord, true, false);
      driver.put(signalRecord, true, false);
      if (!driver.put(migrationRecordBatch, false, true)) {
        AuditLogger.logRuleProcess(methodName, nameSpace, path,
            replicaRule, startTime, new Date(), ResultCode.REJECT.getMsg(), BATCH_MODE);
      }
      AuditLogger.logRuleProcess(methodName, nameSpace, path, replicaRule, startTime, new Date(),
          resultCode.getMsg(), MONITOR_MODE);

      return resultCode;
    } catch (IOException e) {
      LOG.error("Failed {} to createUpdateMap the replicationRule {}.",path, replicaRule, e);
      AuditLogger.logRuleProcess(methodName, nameSpace, path, replicaRule, startTime, new Date(),
          ResultCode.IO_EXCEPTION.getMsg(), MONITOR_MODE);
      return ResultCode.IO_EXCEPTION;
    }
  }

  /** Interface for ZoneService */
  public int checkReplicaInDC(Configuration conf, URI namenode, List<Path> paths,
      MigrationDataCenters dc) throws IOException, InterruptedException {
    return ZoneMoverWithSetReplication.checkWithSetReplication(conf, namenode, paths, dc);
  }

  private static synchronized void createMonitorThread(String nameSpace, String threadName,
      StoreDriver driver, SignalRecord signalRecord, String path, String replicaRule,
      Date startTime) throws IOException {
    LOG.info("Starting monitor thread for {}.", nameSpace);
    Thread monitorThread = new MonitorThread(threadName, new Configuration(),
        ZoneServiceUtil.getNamespaceUri(nameSpace, new Configuration()),
        driver, signalRecord);
    driver.put(signalRecord, true, false);
    monitorThread.start();
    AuditLogger.logRuleProcess("createMonitorThread", nameSpace,
        path, replicaRule, startTime, new Date(),
        ResultCode.CREATE_SUCCESS.getMsg(), MONITOR_MODE);
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
      MigrationRecord migrationRecord = new MigrationRecord(nameSpace, path, MONITOR_MODE);
      SignalRecord signalRecord = new SignalRecord(nameSpace, true);
      if (driver.get(new Query<>(migrationRecord),
          MigrationRecord.class) == null) {
        AuditLogger.logRuleProcess(
            "DeletePathRuleMap", nameSpace,
            path, "N/A", startTime, new Date(),
            ResultCode.NO_MIGRATION_RECORD.getMsg(), MONITOR_MODE);
        return ResultCode.NO_MIGRATION_RECORD;
      } else {
        driver.remove(new Query<>(migrationRecord), MigrationRecord.class);
        driver.put(signalRecord, true, false);
        AuditLogger.logRuleProcess(
            "DeletePathRuleMap", nameSpace,
            path, "N/A", startTime, new Date(),
            ResultCode.SUCCESS.getMsg(), MONITOR_MODE);
        return ResultCode.SUCCESS;
      }
    } catch (IOException e) {
      LOG.error("Fail to remove the rule on {}.", path, e);
      AuditLogger.logRuleProcess(
          "DeletePathRuleMap", nameSpace,
          path, "N/A", startTime, new Date(),
          ResultCode.IO_EXCEPTION.getMsg(), MONITOR_MODE);
      return ResultCode.IO_EXCEPTION;
    }
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