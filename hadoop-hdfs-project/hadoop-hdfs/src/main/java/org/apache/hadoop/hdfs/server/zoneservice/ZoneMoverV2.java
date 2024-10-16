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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.server.balancer.ReplicaDispatcher;
import org.apache.hadoop.hdfs.server.balancer.ExitStatus;
import org.apache.hadoop.hdfs.server.balancer.NameNodeConnector;
import org.apache.hadoop.hdfs.server.mover.Mover;
import org.apache.hadoop.hdfs.server.zoneservice.metrics.ZoneMoverMetrics;
import org.apache.hadoop.hdfs.server.zoneservice.metrics.ZoneProgressTracker;
import org.apache.hadoop.hdfs.server.zoneservice.metrics.ZoneServiceMetrics;
import org.apache.hadoop.hdfs.server.zoneservice.store.KafkaTopicRecord;
import org.apache.hadoop.hdfs.server.zoneservice.store.MigrationRecord;
import org.apache.hadoop.hdfs.server.zoneservice.store.Query;
import org.apache.hadoop.hdfs.server.zoneservice.store.SignalRecord;
import org.apache.hadoop.hdfs.server.zoneservice.store.StoreDriver;
import org.apache.hadoop.hdfs.server.zoneservice.utils.MigrationDataCenters;
import org.apache.hadoop.hdfs.server.zoneservice.utils.RunMode;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ZONESERVICE_STORE_DRIVER_CLASS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ZONESERVICE_STORE_DRIVER_CLASS_DEFAULT;

/** <p>The zonemover is a tool that supports migrating replicas of blocks
 * between datacenters.
 * </p>
 *
 * <pre>
 * To start:
 *      hdfs zonemover -namespace <namespace> -path <path> -rule <rule>
 */
public class ZoneMoverV2 {
  public static final Logger LOG = LoggerFactory.getLogger(ZoneMoverV2.class);
  protected final List<Path> targetPaths;
  protected ReplicationRule globalRule = null;
  protected Map<String, ReplicationRule> pathRuleMap = Maps.newHashMap();
  public final ReplicationRuleUtil ruleUtil;
  private final boolean xattrSetEnable;

  protected final RunMode runMode;

  protected Result result;

  protected static int checkUpdateInterval = 0;

  private boolean enableDR = false;
  protected boolean enableMigrationDC = false;

  protected final DistributedFileSystem dfs;
  private final NameNodeConnector nnc;

  private final String ns;

  /** Enable PreMigration. **/
  protected boolean enablePreMigration;
  protected PreMigrationChecker preMigrationChecker;
  protected CheckFileTaskStatusThead checkFileTaskStatusThead;

  /** Enable Coordinator to increase/decrease replication of files. **/
  protected final ZoneReplicationCoordinator coordinator;
  protected final CoordinatorFetcher coordinatorFetcher;

  /** To schedule and mange ReplicaMoveTask. **/
  private final ReplicaDispatcher replicaDispatcher;

  private final ZoneMoverTrigger zoneMoverTrigger;

  // Initialize ZoneMover Metrics.
  protected ZoneMoverMetrics zoneMoverMetrics = null;
  private ZoneMoverHttpServer httpServer = null;

  public ZoneMoverV2(Configuration conf, URI nameNode, List<Path> paths,
      RunMode runMode, ZoneMoverTrigger zoneMoverTrigger) throws IOException {
    this.targetPaths = paths;
    this.runMode = runMode;
    // Initialize NNC.
    NameNodeConnector nnc = new NameNodeConnector(ZoneMoverV2.class.getSimpleName(),
        nameNode, getIdPath(runMode), paths, conf, 1);
    nnc.getKeyManager().startBlockKeyUpdater();
    this.nnc = nnc;
    this.dfs = nnc.getDistributedFileSystem();
    this.ns = nameNode.getAuthority();;
    // Initialize ReplicaDispatcher.
    this.replicaDispatcher = new ReplicaDispatcher(nnc, conf);
    this.ruleUtil = new ReplicationRuleUtil(this.dfs);

    this.xattrSetEnable = conf.getBoolean(
        DFSConfigKeys.DFS_ZONEMOVER_XATTR_SET_ENABLE_KEY,
        DFSConfigKeys.DFS_ZONEMOVER_XATTR_SET_ENABLE_DEFAULT);
    checkUpdateInterval = conf.getInt(
        DFSConfigKeys.DFS_ZONEMOVER_CHECK_ZK_UPDATE_PATH_RULE_MAP_INTERVAL,
        DFSConfigKeys.DFS_ZONEMOVER_CHECK_ZK_UPDATE_PATH_RULE_MAP_INTERVAL_DEFAULT);
    // Initialize PreMigrationChecker.
    this.preMigrationChecker = new PreMigrationChecker(conf, "ZoneMover-PreMigrationChecker");
    // Initialize Coordinator and Fetcher.
    this.coordinator = new ZoneReplicationCoordinator(conf, this.dfs);
    this.coordinatorFetcher = initCoordinatorFetcher(conf);
    this.zoneMoverTrigger = zoneMoverTrigger;
    if (this.runMode.equals(RunMode.MONITOR)) {
      // Only monitor mode enable CheckFileTaskStatusThead.
      this.checkFileTaskStatusThead = new CheckFileTaskStatusThead(conf,
          "ZoneMover-CheckFileTaskStatus");
    }
  }

  public ZoneMoverV2(Configuration conf, URI nameNode, List<Path> paths,
      RunMode runMode, ZoneMoverTrigger zoneMoverTrigger, ReplicationRule rule)
      throws IOException {
    this(conf, nameNode, paths, runMode, zoneMoverTrigger);
    this.globalRule = rule;
  }

  public ZoneMoverV2(Configuration conf, URI nameNode, List<Path> paths,
      RunMode runMode, ZoneMoverTrigger zoneMoverTrigger,
      Map<String, ReplicationRule> pathRuleMap) throws IOException {
    this(conf, nameNode, paths, runMode, zoneMoverTrigger);
    this.pathRuleMap = pathRuleMap;
  }

  public void setEnableDR(boolean enableDR) {
    this.enableDR = enableDR;
  }

  protected CoordinatorFetcher initCoordinatorFetcher(Configuration conf) {
    return new CoordinatorFetcher("ZoneMover-CoordinatorFetcher");
  }

  /**
   * Migrate multiple paths.
   */
  protected ExitStatus migratePaths() {
    try {
      return processPath(null).getExitStatus();
    } catch (IllegalArgumentException e) {
      System.out.println(e + ".  Exiting ...");
      return ExitStatus.ILLEGAL_ARGUMENTS;
    }
  }

  /**
   * Migrate one path without stop the ZoneMover.
   */
  Result migratePath(String path) {
    Result result = new Result();
    if (this.globalRule != null) {
      processPathWithRule(path, this.globalRule, result, null);
    } else {
      processPathWithRule(path, getPathRule(path), result, null);
    }
    return result;
  }

  /**
   * Get the corresponding rule for the path.
   * @param path to apply a rule
   * @return the corresponding rule
   */
  private ReplicationRule getPathRule(String path)
      throws IllegalArgumentException {
    if (enableDR || (pathRuleMap.isEmpty() && enableMigrationDC)) {
      return null;
    }
    String matchPath = "";
    for (Map.Entry<String, ReplicationRule> entry: pathRuleMap.entrySet()) {
      String key = entry.getKey();
      if (path.startsWith(key)) {
        // A sub path may have different a rule with its parent path.
        // For example, if "/test" and "/test/abc" have different rules,
        // "/test/abc/1.txt" should use the rule of "/test/abc".
        if (key.length() > matchPath.length()) {
          matchPath = key;
        }
      }
    }
    if (!matchPath.isEmpty()) {
      return pathRuleMap.get(matchPath);
    } else {
      throw new IllegalArgumentException("No rule for path: " + path);
    }
  }

  /**
   * Start some daemon threads.
   */
  public void start() {
    if (this.replicaDispatcher != null) {
      this.replicaDispatcher.start();
    }
    if (this.preMigrationChecker != null) {
      this.preMigrationChecker.start();
    }
    if (this.coordinatorFetcher != null) {
      this.coordinatorFetcher.start();
    }
    if (this.checkFileTaskStatusThead != null) {
      this.checkFileTaskStatusThead.start();
    }
  }

  public void initMoverMetrics() throws IOException{
    if (this.zoneMoverMetrics == null) {
      DefaultMetricsSystem.initialize("ZoneMover");
      this.zoneMoverMetrics = ZoneMoverMetrics.create();
      this.httpServer = ZoneUtil.startHttpServer(this.dfs.getConf());
      this.coordinator.setZoneMoverMetrics(this.zoneMoverMetrics);
    }
  }

  /**
   * Return true if this path can be skipped in trigger monitor.
   */
  public boolean doesSkipPath(String path) {
    return false;
  }

  public int startInTriggerMonitor() throws IOException {
    start();
    initMoverMetrics();

    while (this.zoneMoverTrigger.hasNext()) {
      try {
        Pair<ConsumerRecord<String, String>, String> curRecord =
            this.zoneMoverTrigger.getNextRecord();
        // process the path
        String curPath = curRecord.getRight();
        LOG.debug("Start to monitor process path: {}", curPath);
        if (doesSkipPath(curPath)) {
          LOG.info("{} will be skipped.", curPath);
          continue;
        }
        Result result = migratePath(curPath);
        if (this.checkFileTaskStatusThead != null) {
          this.checkFileTaskStatusThead.addFileTask(new FileTask(curPath, result, Time.now(),
              curRecord.getLeft()));
        }
      } catch (InterruptedException e) {
        return ExitStatus.INTERRUPTED.getExitCode();
      }
    }
    return ExitStatus.SUCCESS.getExitCode();
  }

  public int startInZoneServiceTriggerMonitor(ZoneServiceMetrics zoneServiceMetrics) {
    start();
    while (zoneMoverTrigger.hasNext()) {
      try {
        String curPath = zoneMoverTrigger.getNext();
        // process the path
        LOG.debug("Check path: {}", curPath);
        Result result = migratePath(curPath);
        if (result.getExitStatus() != ExitStatus.SUCCESS) {
          zoneServiceMetrics.incrFailMoveCount();
          zoneServiceMetrics.incrNSMonitorFailMoveCount(ns);
          LOG.warn("Monitor process file fail: {}", curPath);
        } else {
          zoneServiceMetrics.incrNSMonitorSuccessMoveCount(this.ns);
          zoneServiceMetrics.incrSuccessMoveCount();
        }
      } catch (IllegalArgumentException e) {
        LOG.warn(e.toString());
      } catch (InterruptedException e) {
        return ExitStatus.INTERRUPTED.getExitCode();
      }
    }
    return ExitStatus.SUCCESS.getExitCode();
  }

  /**
   * Stop all threads.
   */
  public void shutdown() {
    if (this.replicaDispatcher != null) {
      this.replicaDispatcher.shutdown();
    }
    if (preMigrationChecker != null) {
      preMigrationChecker.interrupt();
    }
    if (this.coordinatorFetcher != null) {
      this.coordinatorFetcher.interrupt();
    }
    if (this.checkFileTaskStatusThead != null) {
      this.checkFileTaskStatusThead.interrupt();
    }
    if (this.nnc != null) {
      IOUtils.cleanupWithLogger(LOG, this.nnc);
    }
    if (this.zoneMoverMetrics != null) {
      this.zoneMoverMetrics.shutdown();
      DefaultMetricsSystem.initialize("ZoneMover");
      this.zoneMoverMetrics = null;
    }
    if (httpServer != null) {
      try {
        httpServer.stop();
        this.httpServer = null;
      } catch (Exception e) {
        LOG.error("Exception while stopping httpserver", e);
      }
    }
  }

  /**
   * Run with prepared arguments.
   * @param conf configuration
   * @param namenode URI of the NameNode
   * @param paths paths to apply the rule
   * @param rule the rule to apply
   * @param pathRuleMap map of paths and rules
   * @return a ExitStatus code
   */
  public static int runInBatch(Configuration conf, URI namenode, List<Path> paths,
      ReplicationRule rule, Map<String, ReplicationRule> pathRuleMap)
      throws IOException, InterruptedException {
    ZoneProgressTracker.startCountingInitTime();
    ZoneUtil.checkDataCenterValues(conf, rule, pathRuleMap);
    LOG.info("Start to apply rule: {} to nameNode:{}, path: {}", rule, namenode, paths);
    if (paths.isEmpty()) {
      ZoneProgressTracker.finishCountingInitTimeAndLog();
      return ExitStatus.SUCCESS.getExitCode();
    }

    ZoneMoverV2 zs = null;
    try {
      if (rule != null) {
        zs = new ZoneMoverV2(conf, namenode, paths, RunMode.BATCH, null, rule);
      } else {
        zs = new ZoneMoverV2(conf, namenode, paths, RunMode.BATCH, null, pathRuleMap);
      }
      zs.start();

      ZoneProgressTracker.finishCountingInitTimeAndLog();

      return zs.migratePaths().getExitCode();
    } finally {
      ZoneProgressTracker.checkForLeak();
      if (zs != null) {
        zs.shutdown();
      }
    }
  }

  /**
   * Run with prepared arguments for monitorByTrigger mode.
   * @param conf configuration
   * @param namenode URI of the NameNode
   * @param paths paths to apply the rule
   * @param rule the rule to apply
   * @param pathRuleMap map of paths and rules
   * @return a ExitStatus code
   */
  protected static int runInMonitor(Configuration conf, URI namenode, List<Path> paths,
      ReplicationRule rule, Map<String, ReplicationRule> pathRuleMap,
      boolean loadMapFromStore) throws Exception {
    if (!loadMapFromStore) {
      try {
        if (ExitStatus.SUCCESS.getExitCode() !=
            runInBatch(conf, namenode, paths, rule, pathRuleMap)) {
          LOG.error("Move the original data for {} in {} fail.", paths, namenode.getAuthority());
        }
      } catch (InterruptedException e) {
        LOG.error("Batch process is interrupted.", e);
      }
    }
    ZoneUtil.checkDataCenterValues(conf, rule, pathRuleMap);
    if (paths.isEmpty() && !loadMapFromStore) {
      return ExitStatus.SUCCESS.getExitCode();
    }
    // Initialize ZoneService Metrics
    ZoneServiceMetrics zoneServiceMetrics;
    if (loadMapFromStore) {
      zoneServiceMetrics = ZoneService.getMetrics();
    } else {
      zoneServiceMetrics = ZoneServiceMetrics.create();
    }

    Class<? extends StoreDriver> driverClass = conf.getClass(
        DFS_ZONESERVICE_STORE_DRIVER_CLASS,
        DFS_ZONESERVICE_STORE_DRIVER_CLASS_DEFAULT,
        StoreDriver.class);
    ZoneMoverV2 zs = null;
    Thread mapUpdaterThread = null;
    StoreDriver driver = null;
    LOG.info("Initializing NameNodeConnector");
    try {
      if (rule != null) {
        zs = new ZoneMoverV2(conf, namenode, paths,
            RunMode.MONITOR, new ZoneMoverKafkaTrigger(conf, paths, namenode), rule);
      } else {
        zs = new ZoneMoverV2(conf, namenode, paths,
            RunMode.MONITOR, new ZoneMoverKafkaTrigger(conf, paths, namenode), pathRuleMap);
      }
      // Monitor if the path rule map is update or not when zk enable
      if (loadMapFromStore) {
        LOG.info("Initializing MapUpdater");
        driver = ReflectionUtils.newInstance(driverClass, conf);
        driver.init(conf, "ZoneMoverV2_" + namenode.getAuthority());
        mapUpdaterThread = zs.startMapUpdater(namenode, driver);
      }
      return zs.startInZoneServiceTriggerMonitor(zoneServiceMetrics);
    } finally {
      if (zs != null) {
        zs.shutdown();
      }
      if (mapUpdaterThread != null) {
        mapUpdaterThread.interrupt();
      }
      if (driver != null) {
        driver.close();
      }
    }
  }

  protected Path getIdPath(RunMode mode) {
    return new Path(String.format("%s.%s.%s.%s",
        "/system/zonemover.id",
        mode.toString().toLowerCase(),
        NetUtils.getLocalHostname(),
        Time.now()));
  }

  public ZoneMoverMetrics getZoneMoverMetrics() {
    return this.zoneMoverMetrics;
  }

  /**
   * Check if a block already satisfies the rule.
   */
  boolean isBlockSatisfyRule(LocatedBlock block, ReplicationRule rule) {
    return rule.equals(ReplicationRule.parseFromMap(
        ZoneUtil.getBlockDistribution(block)));
  }

  /**
   * Recursively scan all files and submit move tasks.
   */
  public Result processPath(MigrationDataCenters dc) {
    ZoneProgressTracker.resetTracker();
    ZoneProgressTracker.trackPaths(this.dfs, this.targetPaths);
    Result result = new Result();
    this.result = result;

    ZoneProgressTracker.startCountingProcessPathTime();
    // Recursively scan all files and submit move tasks.
    for (Path target : this.targetPaths) {
      String path = target.toUri().getPath();
      if (doesSkipPath(path)) {
        LOG.info("Skipping blacklisted path {}.", path);
        continue;
      }
      if (this.globalRule != null) {
        processPathWithRule(path, this.globalRule, result, dc);
      } else {
        processPathWithRule(path, getPathRule(path), result, dc);
      }
    }
    ZoneProgressTracker.finishCountingProcessPathTimeAndLog();

    // Stop and waiting for both coordinator and preMigration to finish.
    stopCoordinatorAndPreMigration();

    // wait for pending move to finish and retry the failed migration
    ZoneProgressTracker.startCountingMoveCompletionWaitTime();
    this.replicaDispatcher.waitForMoveCompletion();
    ZoneProgressTracker.finishCountingMoveCompletionWaitTimeAndLog();

    return result;
  }

  // stop coordinator after pre-process queue is empty
  protected void stopCoordinatorAndPreMigration() {
    try {
      // Stop and wait for preMigration to finish.
      waitPreMigrationCheckerCompletion();

      // Stop and wait for coordinator to finish
      coordinator.waitForCheckCompletion();

      // Wait the fetcher thread to finish.
      ZoneProgressTracker.startCountingFetcherWaitTime();
      try {
        coordinatorFetcher.join();
      } finally {
        ZoneProgressTracker.finishCountingFetcherWaitTimeAndLog();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void waitPreMigrationCheckerCompletion() throws InterruptedException {
    if (preMigrationChecker != null) {
      while (!preMigrationChecker.preMigrationFileQueue.isEmpty()) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          break;
        }
      }
      preMigrationChecker.setShouldServiceStop();
      preMigrationChecker.join();
    }
  }

  /**
   * Listing directory in batch and process all files or directories.
   */
  protected void processPathWithRule(String fullPath, ReplicationRule rule,
      Result result, MigrationDataCenters dc) {
    byte[] lastReturnedName = HdfsFileStatus.EMPTY_NAME;
    while (true) {
      final DirectoryListing children;
      try {
        children = dfs.getClient().listPaths(fullPath, lastReturnedName, true);
      } catch (IOException e) {
        LOG.warn("Failed to list directory {}. Ignore the directory and continue.",
            fullPath, e);
        return;
      }
      if (children == null) {
        return;
      }
      for (HdfsFileStatus child : children.getPartialListing()) {
        processPathWithRuleRecursively(fullPath, child, rule, result, dc);
      }
      if (children.hasMore()) {
        lastReturnedName = children.getLastName();
      } else {
        return;
      }
    }
  }

  /**
   * Recursively scan all files and submit move tasks.
   */
  private void processPathWithRuleRecursively(String parent, HdfsFileStatus status,
      ReplicationRule rule, Result result, MigrationDataCenters dc) {
    String fullPath = status.getFullName(parent);
    if (doesSkipPath(fullPath)) {
      LOG.info("Skipping blacklisted path {}.", fullPath);
      return;
    }
    if (status.isDir()) {
      processPathWithRule(fullPath, rule, result, dc);
    } else if (!status.isSymlink()) { // file
      processFileWithPreMigration(fullPath, (HdfsLocatedFileStatus) status, rule, result);
    }
  }

  private void processFileById(String fullPath, long fileId, Result result) throws IOException {
    HdfsFileStatus[] statuses = dfs.getClient().listPaths(fullPath, fileId,
        HdfsFileStatus.EMPTY_NAME, true).getPartialListing();

    if (statuses[0].isDir()) {
      LOG.debug("Skip directory: {}", fullPath);
      return;
    }
    Preconditions.checkArgument(statuses[0] instanceof HdfsLocatedFileStatus);
    HdfsLocatedFileStatus status = (HdfsLocatedFileStatus) statuses[0];
    final LocatedBlocks locatedBlocks = status.getLocatedBlocks();
    if (status.getLen() == 0) {
      LOG.debug("Skip empty file: {}", fullPath);
      return;
    }

    if (!locatedBlocks.isLastBlockComplete()) {
      LOG.debug("Skip uncompleted file: {}", fullPath);
      return;
    }

    ReplicationRule rule = ReplicationRule.parseFromString(
        ruleUtil.getStringFromRuleKey(fullPath, fileId).replace("\"", ""));

    try {
      if (status.getReplication() != rule.getReplica()) {
        Thread.sleep(2000);
      }
    } catch (InterruptedException ignored) {
    }
    processFileDirectly(fullPath, status, rule, result);
  }

  protected boolean canSkip(String fullPath, HdfsLocatedFileStatus status) {
    final LocatedBlocks locatedBlocks = status.getLocatedBlocks();
    // Skip empty file.
    if (status.getLen() == 0) {
      LOG.debug("Skip empty file: {}.", fullPath);
      ZoneProgressTracker.incrFileCount();
      return true;
    }

    // Skip uncompleted file.
    if (!locatedBlocks.isLastBlockComplete()) {
      LOG.debug("Skip uncompleted file: {}.", fullPath);
      ZoneProgressTracker.incrFileCount();
      return true;
    }
    return false;
  }

  /**
   * This method may do the pre-migration first if needed.
   * Usually this method is used to migrate a replica of files
   * to target data center first to reduce bandwidth across data center.
   */
  protected void processFileWithPreMigration(String fullPath,
      HdfsLocatedFileStatus status, ReplicationRule targetRule, Result result) {
    processFileWithSetReplication(fullPath, status, targetRule, result);
  }

  /**
   * This method is used to increase or decrease the replication of files
   * to make the number of replication same with the target rule.
   * @return true if this file will be migrated by Fetcher.
   */
  protected boolean processFileWithSetReplication(String fullPath,
      HdfsLocatedFileStatus status, ReplicationRule targetRule, Result result) {
    LOG.debug("Processing file: {}...", fullPath);
    if (canSkip(fullPath, status)) {
      return false;
    }

    // Maybe this logic can be deleted.
    if (xattrSetEnable) {
      boolean needSet = true;
      try {
        if (ruleUtil.hasRuleInXAttr(fullPath)) {
          try {
            if (ruleUtil.getRuleFromXAttr(fullPath).equals(targetRule)) {
              LOG.debug("This file already has the replicationRule: {}", fullPath);
              needSet = false;
            }
          } catch (IllegalArgumentException e) {
            LOG.warn("Found an invalid rule in NameNode, will update it: file={}, content={}",
                fullPath, ruleUtil.getStringFromRuleKey(fullPath));
          }
        }

        if (needSet) {
          ruleUtil.setRuleToXAttr(fullPath, targetRule);
          LOG.debug("Added replicationRule to: {}", fullPath);
        }
      } catch (IOException e) {
        LOG.warn(e.toString());
        ZoneProgressTracker.incrFileCount();
        return false;
      }
    } else if (status.getReplication() != targetRule.getReplica()) {
      // Why not increase or decrease the replication of files?
      LOG.warn("Ignore replica not consistent file: {}", fullPath);
      ZoneProgressTracker.incrFileCount();
      return false;
    }

    final LocatedBlocks locatedBlocks = status.getLocatedBlocks();
    ZoneProgressTracker.queueFile(fullPath);
    if (status.getReplication() < targetRule.getReplica()) {
      LOG.debug("factor < rule.replication file: {}", fullPath);
      // For cases like changing "/Telin-3:3" to "/Telin-3:3,/Telin-4:2",
      // ZoneMover does not need to wait for NameNode to schedule replication.
      // For cases like changing "/Telin-3:3" to "/Telin-3:2,/Telin-4:3",
      // ZoneMover needs to wait for NameNode to schedule first and then
      // do redistribution. If ZoneMover does not wait, it will increase
      // the number of inter-dc block replications.
      if (ZoneUtil.isFileNeedMove(locatedBlocks, targetRule)) {
        coordinator.addFile(fullPath, targetRule,
            (targetRule.getReplica() - status.getReplication()),
            locatedBlocks.getLocatedBlocks().size());
        return true;
      }
    } else if (status.getReplication() > targetRule.getReplica()) {
      LOG.debug("factor > rule.replication file: {}", fullPath);
      // For cases like changing "/Telin-3:3,/Telin-4:2" -> "/Telin-3:3",
      // ZoneMover does not need to wait.
      // For cases like changing "/Telin-3:3" to "/Telin-3:1,/Telin-4:1",
      // ZoneMover needs to wait. Otherwise, NameNode may schedule the
      // datanode to delete the replica first, which is also used as the
      // proxy of replaceBlock scheduled by ZoneMover.
      if (ZoneUtil.isFileNeedMove(locatedBlocks, targetRule)) {
        coordinator.addFile(fullPath, targetRule,
            targetRule.getReplica() - status.getReplication(),
            locatedBlocks.getLocatedBlocks().size());
        return true;
      }
    } else {
      try {
        processFileDirectly(fullPath, status, targetRule, result);
      } finally {
        ZoneProgressTracker.dequeueFile(fullPath);
      }
    }
    return false;
  }

  /**
   * Return true if the current distribution is valid.
   */
  protected boolean isValidDistribution(Map<String, Short> currentDistribution) {
    return true;
  }

  /**
   * Migrating replications of blocks accoridng to target rule.
   */
  protected void processLocatedBlock(String fullPath, LocatedBlock lb,
      ReplicationRule targetRule, Result result, ErasureCodingPolicy ecPolicy) {
    Map<String, Short> currentDistribution = ZoneUtil.getBlockDistribution(lb);
    if (!isValidDistribution(currentDistribution)) {
      LOG.debug("Block: {} cannot generate valid replica rule for file: {}", lb, fullPath);
      return;
    }

    ReplicationRule currentRule = ReplicationRule.parseFromMap(currentDistribution);
    if (targetRule.equals(currentRule)) {
      LOG.debug("Block: {} rule: {} is expected will skip for file: {}",
          lb, currentRule, fullPath);
      return;
    }
    List<MoveItemTask> moveTasks = scheduleMoves4Block(fullPath, lb,
        ZoneUtil.getZoneMoveItems(currentDistribution, targetRule), ecPolicy);
    // For monitor mode result maybe as null.
    if (result == null) {
      return;
    }
    if (!moveTasks.isEmpty()) {
      if (this.runMode.equals(RunMode.MONITOR)) {
        result.addMoveTasks(moveTasks);
      }
      result.setNoBlockMoved(false);
    } else {
      result.updateHasRemaining(true);
    }
  }

  /**
   * This method is used to directly migrate blocks according to the target rule.
   */
  protected void processFileDirectly(String fullPath, HdfsLocatedFileStatus status,
      ReplicationRule targetRule, Result result) {
    long beginTime = Time.monotonicNow();
    for (LocatedBlock block : status.getLocatedBlocks().getLocatedBlocks()) {
      processLocatedBlock(fullPath, block, targetRule, result, null);
    }
    if (this.zoneMoverMetrics != null) {
      this.zoneMoverMetrics.addScheduledFiles((Time.monotonicNow() - beginTime));
    }
  }

  /**
   * Migrate replicas of a block according to the moveItems.
   */
  protected List<MoveItemTask> scheduleMoves4Block(String fullPath, LocatedBlock lb,
      List<ZoneMoveItem> moveItems, ErasureCodingPolicy ecPolicy) {
    final Map<String, List<DatanodeInfo>> locationMap =
        ZoneUtil.getBlockDistributionDNs(lb);

    if (lb instanceof LocatedStripedBlock && ecPolicy == null) {
      LOG.warn("Failed to move blocks for {} since ecPolicy is null.",
          lb.getBlock());
      return new ArrayList<>();
    }

    // Avoid case ConcurrentModificationException.
    List<Node> excludedNodes = new CopyOnWriteArrayList<>(Arrays.asList(lb.getLocations()));
    List<MoveItemTask> moveTasks = new ArrayList<>();
    for (ZoneMoveItem moveItem : moveItems) {
      for (short i = 0; i < moveItem.getNum(); i++) {
        List<DatanodeInfo> sourceDNs = locationMap.get(moveItem.getSourceDataCenter());
        DatanodeInfo sourceDN = sourceDNs.get(0);
        sourceDNs.remove(0);
        try {
          LOG.info("Migrate block {} from {} to {} dc for {}.", lb.getBlock(),
              sourceDN, moveItem.getTargetDataCenter(), fullPath);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Excluded nodes {} for block {}", excludedNodes, lb.getBlock());
          }
          ReplicaDispatcher.ReplicaMoveTask task = replicaDispatcher.dispatchLocatedBlock(
              fullPath, sourceDN, lb, moveItem.getTargetDataCenter(), ecPolicy, excludedNodes);
          moveTasks.add(new MoveItemTask(task, lb, fullPath));
        } catch (Exception e) {
          LOG.error("Failed to migrate replica from {} to {} for block {} in {}.",
              sourceDN, moveItem.getTargetDataCenter(), lb.getBlock(), fullPath, e);
          // If build move task fails will create failed MoveItemTask for retrying.
          moveTasks.add(new MoveItemTask(fullPath, lb, sourceDN,
              moveItem.getTargetDataCenter(), excludedNodes, ecPolicy));
        }
      }
    }
    return moveTasks;
  }

  /**
   * A thread that gets the replication changed files from the coordinator and migrate it.
   */
  protected class CoordinatorFetcher extends Thread {
    CoordinatorFetcher(String name) {
      super(name);
    }

    @Override
    public void run() {
      ZoneReplicationCoordinator.FileState fileState = null;
      while (true) {
        try {
          fileState = coordinator.getNextFinishedFile();
          if (zoneMoverMetrics != null) {
            zoneMoverMetrics.addChangeReplicationTotalTime(
                (Time.monotonicNow() - fileState.getCreatingTime()));
          }
          processFileDirectly(fileState.getFilePath(), fileState.getFileStatus(),
              fileState.getRule(), result);
        } catch (NoSuchElementException e) {
          LOG.warn("No more files!", e);
          break;
        } catch (Exception e) {
          LOG.warn("Fetcher encountered the exception!", e);
        } finally {
          if (fileState != null) {
            ZoneProgressTracker.dequeueFile(fileState.getFilePath());
            fileState = null;
          }
        }
      }
    }
  }

  public Thread startMapUpdater(URI namenode, StoreDriver driver) {
    MapUpdater mapUpdater = new MapUpdater(namenode, driver, this.zoneMoverTrigger);
    Thread mapUpdaterThread = new Thread(mapUpdater, "Updater" + namenode.getAuthority());
    mapUpdaterThread.start();
    return mapUpdaterThread;
  }

  /**
   * Monitor records in zookeeper and sync the records.
   */
  public class MapUpdater implements Runnable {
    private final URI namenode;
    private final StoreDriver driver;
    private final ZoneMoverTrigger zoneMoverTrigger;

    public MapUpdater(URI namenode, StoreDriver driver,
        ZoneMoverTrigger zoneMoverTrigger) {
      this.namenode = namenode;
      this.driver = driver;
      this.zoneMoverTrigger = zoneMoverTrigger;
    }

    @Override
    public void run() {
      while(true) {
        try {
          SignalRecord signalRecord = new SignalRecord(namenode.getAuthority());
          if (driver.get(new Query<>(signalRecord), SignalRecord.class)
              .isNeedUpdate()) {
            updatePathRuleMap(driver, namenode.getAuthority(), zoneMoverTrigger);
            signalRecord.finishUpdate();
            driver.put(signalRecord, true, false);
            //noinspection BusyWait
            Thread.sleep(checkUpdateInterval * 1000L);
          }
        } catch (IOException e) {
          LOG.error("There are some errors happen when ZoneMover updates path-rule pairs.", e);
        } catch (InterruptedException e) {
          LOG.warn("Monitor path rule map process is interrupted!");
          break;
        }
      }
    }

    private void updatePathRuleMap(StoreDriver driver, String nameSpace,
        ZoneMoverTrigger zoneMoverTrigger) throws IOException {
      Map<String, ReplicationRule> pathRuleMapTmp = new HashMap<>();
      List<MigrationRecord> records =
          driver.getAll(MigrationRecord.class).getRecords();
      for (MigrationRecord record : records) {
        if (record.getMode().equals("monitor")) {
          if (record.getNs().equals(nameSpace)) {
            pathRuleMapTmp.put(record.getPath(),
                ReplicationRule.parseFromString(record.getRule()));
          }
        }
      }
      pathRuleMap = pathRuleMapTmp;
      zoneMoverTrigger.updatePaths(ZoneUtil.getPaths(pathRuleMap));
    }
  }

  /**
   * A class that records some information of files, a replica of which is migrated first.
   */
  protected static class PreMigrationFile {
    private final String filePath;
    private final long recordTime;
    private final ReplicationRule targetRule;

    PreMigrationFile(String filePath, ReplicationRule targetRule) {
      this.filePath = filePath;
      this.targetRule = targetRule;
      recordTime = Time.monotonicNow();
    }

    public String getFilePath() {
      return filePath;
    }

    public long getRecordTime() {
      return recordTime;
    }

    public ReplicationRule getTargetRule() {
      return targetRule;
    }
  }

  /**
   * A class records the hdfs path task and the kafka record,
   * which is used to detect whether the file execution is successful or failed.
   */
  protected static class FileTask {
    private final String filePath;
    private final Result result;
    private final long startTime;
    private final ConsumerRecord<String, String> kafkaRecord;

    FileTask(String filePath, Result result, long startTime,
        ConsumerRecord<String, String> kafkaRecord) {
      this.filePath = filePath;
      this.result = result;
      this.startTime = startTime;
      this.kafkaRecord = kafkaRecord;
    }

    public Result getResult() {
      return result;
    }

    public String getFilePath() {
      return filePath;
    }

    public long getStartTime() {
      return startTime;
    }

    public ConsumerRecord<String, String> getKafkaRecord() {
      return kafkaRecord;
    }

    @Override
    public int hashCode() {
      return Objects.hash(filePath);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof FileTask)) {
        return false;
      }
      FileTask that = (FileTask)o;
      return Objects.equals(this.filePath, that.filePath);
    }
  }

  public static class MoveItemTask {
    private final String fullPath;
    private final LocatedBlock locatedBlock;
    private String preferDC;
    private DatanodeInfo source;
    private List<Node> excludeNodes;
    private ErasureCodingPolicy ecPolicy;
    private ReplicaDispatcher.ReplicaMoveTask replicaMoveTask;
    private final AtomicInteger retryCount = new AtomicInteger(0);
    private volatile long runningTime = Time.monotonicNow();

    public MoveItemTask(ReplicaDispatcher.ReplicaMoveTask replicaMoveTask,
        LocatedBlock locatedBlock, String fullPath) {
      this.fullPath = fullPath;
      this.locatedBlock = locatedBlock;
      this.replicaMoveTask = replicaMoveTask;
    }

    public MoveItemTask(String fullPath, LocatedBlock locatedBlock, DatanodeInfo source,
        String preferDC, List<Node> excludeNodes, ErasureCodingPolicy ecPolicy) {
      this.fullPath = fullPath;
      this.source = source;
      this.preferDC = preferDC;
      this.excludeNodes = excludeNodes;
      this.locatedBlock = locatedBlock;
      this.ecPolicy = ecPolicy;
    }

    /**
     * Return true if caller can retry this task again.
     */
    public boolean canRetry(long timeout) {
      if (replicaMoveTask != null) {
        return replicaMoveTask.canRetry(timeout);
      } else {
        return timeout > 0 && (Time.monotonicNow() - runningTime > timeout);
      }
    }

    public void setRunningTime(long runningTime) {
      this.runningTime = runningTime;
    }

    public void incRetryCount() {
      retryCount.incrementAndGet();
    }

    public AtomicInteger getRetryCount() {
      return retryCount;
    }

    public void setReplicaMoveTask(ReplicaDispatcher.ReplicaMoveTask replicaMoveTask) {
      this.replicaMoveTask = replicaMoveTask;
    }

    public ReplicaDispatcher.ReplicaMoveTask getReplicaMoveTask() {
      return replicaMoveTask;
    }

    public DatanodeInfo getSource() {
      return source;
    }

    public String getFullPath() {
      return fullPath;
    }

    public LocatedBlock getLocatedBlock() {
      return locatedBlock;
    }

    public String getPreferDC() {
      return preferDC;
    }

    public List<Node> getExcludeNodes() {
      return excludeNodes;
    }

    public ErasureCodingPolicy getEcPolicy() {
      return ecPolicy;
    }
  }

  public static class Result extends Mover.Result {

    private final List<MoveItemTask> moveTasks;

    public Result() {
      super();
      moveTasks = new ArrayList<>();
    }

    public List<MoveItemTask> getMoveTasks() {
      return moveTasks;
    }

    public void addMoveTasks(List<MoveItemTask> tasks) {
      this.moveTasks.addAll(tasks);
    }
  }

  /**
   * Check if the pre-migration file has waited for enough time to proceed to the next step
   */
  protected class PreMigrationChecker extends Thread {
    private volatile boolean running;
    private final BlockingQueue<PreMigrationFile> preMigrationFileQueue;
    private final long preMigrationCheckInterval;
    private volatile boolean shouldServiceStop;

    public PreMigrationChecker(Configuration conf, String name) {
      super(name);
      this.running = true;
      this.shouldServiceStop = false;
      int preMigrationQueueSize = conf.getInt(
          DFSConfigKeys.DFS_ZONE_MIGRATION_PRE_MIGRATION_QUEUE_SIZE_KEY,
          DFSConfigKeys.DFS_ZONE_MIGRATION_PRE_MIGRATION_QUEUE_SIZE_DEFAULT);
      this.preMigrationFileQueue = new LinkedBlockingQueue<>(preMigrationQueueSize);
      this.preMigrationCheckInterval = conf.getLong(
          DFSConfigKeys.DFS_ZONE_MIGRATION_PRE_MIGRATION_CHECK_INTERVAL_KEY,
          DFSConfigKeys.DFS_ZONE_MIGRATION_PRE_MIGRATION_CHECK_INTERVAL_DEFAULT);
    }

    public void stopPreMigrationChecker() {
      this.running = false;
    }

    public void setShouldServiceStop() {
      this.shouldServiceStop = true;
    }

    public void preMigrateFile(PreMigrationFile file)
        throws InterruptedException {
      this.preMigrationFileQueue.put(file);
    }

    @Override
    public void run() {
      LOG.info("PreMigrationChecker is starting....");
      PreMigrationFile preMigrationFile = null;
      while (this.running || !this.preMigrationFileQueue.isEmpty()) {
        try {
          preMigrationFile = this.preMigrationFileQueue.poll();
          if (preMigrationFile != null) {
            if (zoneMoverMetrics != null) {
              zoneMoverMetrics.decrPendingPreMigration();
            }
            long sleepTime = this.preMigrationCheckInterval -
                (Time.monotonicNow() - preMigrationFile.getRecordTime());
            if (sleepTime <= 0) {
              HdfsLocatedFileStatus status = (HdfsLocatedFileStatus)
                  dfs.getClient().listPaths(preMigrationFile.getFilePath(),
                          HdfsFileStatus.EMPTY_NAME, true)
                      .getPartialListing()[0];
              processFileWithSetReplication(preMigrationFile.getFilePath(),
                  status, preMigrationFile.getTargetRule(), result);
            } else {
              preMigrationFileQueue.put(preMigrationFile);
              Thread.sleep(sleepTime);
            }
          } else {
            if (shouldServiceStop) {
              LOG.info("PreMigrationChecker will be stopped");
              stopPreMigrationChecker();
            }
            Thread.sleep(this.preMigrationCheckInterval);
          }
        } catch (Exception e) {
          LOG.warn("Pre-migration checker encountered the exception!", e);
        } finally {
          if (preMigrationFile != null) {
            ZoneProgressTracker.dequeueFile(preMigrationFile.getFilePath());
            preMigrationFile = null;
          }
        }
      }
    }
  }

  /**
   * The class will to check if the migration file task succeeded or failed for trigger mode.
   */
  protected class CheckFileTaskStatusThead extends Thread {
    private static final long RECHECK_INTERVAL = 10000;
    private final ConcurrentLinkedDeque<FileTask> fileTasks;
    private final int checkLimit;
    private final long retryTimeout;
    private final int maxRetryCount;

    public CheckFileTaskStatusThead(Configuration conf, String name) {
      super(name);
      this.fileTasks = new ConcurrentLinkedDeque<>();
      this.checkLimit = conf.getInt(DFSConfigKeys.DFS_ZONEMOVER_CHECK_PATH_LIMIT_KEY,
          DFSConfigKeys.DFS_ZONEMOVER_CHECK_PATH_LIMIT_DEFAULT);
      this.retryTimeout = conf.getLong(DFSConfigKeys.DFS_ZONEMOVER_TASK_RETRY_TIMEOUT_MS,
          DFSConfigKeys.DFS_ZONEMOVER_TASK_RETRY_TIMEOUT_MS_DEFAULT);
      this.maxRetryCount = conf.getInt(DFSConfigKeys.DFS_ZONEMOVER_TASK_RETRY_COUNT,
          DFSConfigKeys.DFS_ZONEMOVER_TASK_RETRY_COUNT_DEFAULT);
    }

    public void addFileTask(FileTask fileTask) {
      this.fileTasks.addLast(fileTask);
    }

    @Override
    public void run() {
      LOG.info("CheckFileTaskStatusThead is starting....");
      while (!Thread.currentThread().isInterrupted()) {
        try {
          processCheckFiles();
          Thread.sleep(RECHECK_INTERVAL);
        } catch (InterruptedException ie) {
          LOG.warn("CheckFileTaskStatusThead interrupted will stop", ie);
          Thread.currentThread().interrupt();
        } catch (Exception e) {
          LOG.warn("CheckFileTaskStatusThead encountered the exception!", e);
        }
      }
    }

    private void processCheckFiles() {
      int loopCount = 0;
      List<FileTask> toRemove = new ArrayList<>();
      FileTask lastCompletedCheckFile = null;
      for (FileTask fileTask : fileTasks) {
        if (loopCount >= checkLimit) {
          break;
        }
        if (fileTask.getResult().isNoBlockMoved()) {
          LOG.debug("No need to process path: {}", fileTask.getFilePath());
          toRemove.add(fileTask);
          if (zoneMoverMetrics != null) {
            zoneMoverMetrics.incrSkippedFiles();
          }
          continue;
        }

        if (processMoveTasks(fileTask, toRemove)) {
          if (lastCompletedCheckFile == null || fileTask.getKafkaRecord().offset() >
              lastCompletedCheckFile.getKafkaRecord().offset()) {
            lastCompletedCheckFile = fileTask;
          }
        }
        loopCount++;
      }

      fileTasks.removeAll(toRemove);
      updateKafkaOffsets(lastCompletedCheckFile);
    }

    /**
     * Handles checking the status of move tasks, determines if tasks were successful,
     * failed or need to be retried and updating Kafka offsets.
     */
    private boolean processMoveTasks(FileTask fileTask, List<FileTask> toRemove) {
      List<MoveItemTask> moveTasks = fileTask.getResult().getMoveTasks();
      List<MoveItemTask> removeTasks = new ArrayList<>();
      boolean isSuccess = true;
      boolean isFailed = false;
      long endTime = 0;

      for (int i = 0; i < moveTasks.size(); i++) {
        MoveItemTask moveItemTask = moveTasks.get(i);
        if (moveItemTask == null) {
          continue;
        }
        ReplicaDispatcher.ReplicaMoveTask replicaMoveTask = moveItemTask.getReplicaMoveTask();
        if (replicaMoveTask != null && replicaMoveTask.getTaskState().equals(
            ReplicaDispatcher.ReplicaMoverTaskState.SUCCESS)) {
          removeTasks.add(moveItemTask);
          if (replicaMoveTask.getEndTime() > endTime) {
            endTime = replicaMoveTask.getEndTime();
          }
        } else {
          isSuccess = false;
          if (replicaMoveTask == null || replicaMoveTask.getTaskState().equals(
              ReplicaDispatcher.ReplicaMoverTaskState.FAILED)) {
            if (moveItemTask.getRetryCount().get() >= maxRetryCount) {
              LOG.debug("{} execute task failed.", fileTask.getFilePath());
              isFailed = true;
              break;
            } else if (moveItemTask.canRetry(retryTimeout)) {
              LOG.debug("{} need to retry execute task.", fileTask.getFilePath());
              moveTasks.set(i, retryExecuteTask(moveItemTask));
            }
          }
        }
      }
      boolean isCompletedCheck = isSuccess || isFailed;
      if (isCompletedCheck) {
        if (zoneMoverMetrics != null) {
          if (isSuccess) {
            LOG.debug("Success to process path: {} .", fileTask.getFilePath());
            zoneMoverMetrics.addSuccessFiles(endTime - fileTask.getStartTime());
          } else {
            zoneMoverMetrics.incrFailedFiles();
            LOG.warn("Failed to process path: {} will record zk.", fileTask.getFilePath());
            zoneMoverTrigger.savePathRecordToZookeeper(ns, fileTask.getFilePath());
          }
        }
        toRemove.add(fileTask);
      } else if (!removeTasks.isEmpty()) {
        moveTasks.removeAll(removeTasks);
      }
      return isCompletedCheck;
    }

    /**
     * Updates the Kafka offsets to Zookeeper based on the current processing state.
     * Get the first element in the queue, if present will set to `record.offset() - 1`,
     * indicating that the previous record has been processed,
     * otherwise will use offset of lastSuccessCheckFile.
     * @param lastCompletedCheckFile
     */
    private void updateKafkaOffsets(FileTask lastCompletedCheckFile) {
      FileTask fileTask = fileTasks.peekFirst();
      FileTask offsetCheckFile = (fileTask != null) ? fileTask : lastCompletedCheckFile;

      if (offsetCheckFile != null) {
        ConsumerRecord<String, String> record = offsetCheckFile.getKafkaRecord();
        KafkaTopicRecord kafkaTopicRecord = new KafkaTopicRecord(ns, record.topic(),
            zoneMoverTrigger.getGroupId(), record.partition(),
            fileTask != null ? record.offset() - 1 : record.offset());
        long duration = zoneMoverTrigger.saveOffsetToZookeeperCommon(kafkaTopicRecord);
        if (duration != -1 && zoneMoverMetrics != null) {
          zoneMoverMetrics.addKafkaOffsetZk(duration);
        }
      } else {
        LOG.warn("No valid FileTask found to update Kafka offsets.");
      }
    }

    private MoveItemTask retryExecuteTask(MoveItemTask moveItemTask) {
      if (moveItemTask == null) {
        return null;
      }

      try {
        if (moveItemTask.getReplicaMoveTask() != null) {
          replicaDispatcher.dispatchMoveTask(moveItemTask.getFullPath(),
              moveItemTask.getReplicaMoveTask());
        } else {
          ReplicaDispatcher.ReplicaMoveTask movingTask = replicaDispatcher.
              dispatchLocatedBlock(moveItemTask.getFullPath(),
              moveItemTask.getSource(), moveItemTask.getLocatedBlock(),
              moveItemTask.getPreferDC(), moveItemTask.getEcPolicy(),
              moveItemTask.getExcludeNodes());
          moveItemTask.setReplicaMoveTask(movingTask);
        }
      } catch (IOException e) {
        LOG.error("Failed retry execute task {} for {} with error, ",
            moveItemTask.getLocatedBlock(), moveItemTask.getFullPath(), e);
      } finally {
        moveItemTask.incRetryCount();
        moveItemTask.setRunningTime(Time.monotonicNow());
      }
      return moveItemTask;
    }
  }

  protected static class Cli extends Configured implements Tool {
    private static final String USAGE = "Usage: hdfs zonemover"
        + "\n\t[-namespace <namespace>]\tthe namespace to apply the rule"
        + "\n\t-path <path>\tthe path to apply the rule"
        + "\n\t-pathFile <pathFile>\t the file contains paths to apply the rule"
        + "\n\t-rule <rule>\tthe replication rule"
        + "\n\t-pathRuleFile <pathRuleFile> the file contains path and rule mappings."
        + "\n\t               Path and rule are separated by space in each line."
        + "\n\t-monitor\tenable monitor mode"
        + "\n\t-monitorByTrigger\tenable monitor mode with trigger";

    private static Options buildCliOptions() {
      Options options = new Options();
      Option option = new Option(
          null, "namespace", true, "the namespace to apply the rule");
      options.addOption(option);

      OptionGroup pathGroup = new OptionGroup();
      pathGroup.setRequired(true);
      // For commons-cli 1.2, options with "longOpt" but without "opt"
      // in an OptionGroup do not have the "mutually exclusive" feature.
      // So "opt" and "longOpt" are same below. This has been fixed
      // in commons-cli 1.5.0 .
      option = new Option("path", "path", true, "the path to apply the rule");
      pathGroup.addOption(option);
      option = new Option("pathFile", "pathFile", true,
          "the file contains paths to apply the rule");
      pathGroup.addOption(option);
      Option pathRuleOption = new Option("pathRuleFile", "pathRuleFile", true,
          "the file contains path and rule mappings");
      pathGroup.addOption(pathRuleOption);
      options.addOptionGroup(pathGroup);

      option = new Option(null, "rule", true, "the replication rule");
      options.addOption(option);

      option = new Option(null, "monitor", false, "enable monitor mode");
      options.addOption(option);

      option = new Option(null, "monitorByTrigger", false, "enable monitor by trigger");
      options.addOption(option);
      return options;
    }

    private static void additionalOptionsCheck(CommandLine line)
        throws IllegalArgumentException {
      // As commons-cli 1.2 does not support adding one option
      // to two OptionGroups, we need to check this in a trick way.
      if ((line.hasOption("rule") && line.hasOption("pathRuleFile")) ||
          (!line.hasOption("rule") && !line.hasOption("pathRuleFile"))) {
        throw new IllegalArgumentException
            ("'-rule' and '-pathRuleFile' CAN and MUST specify one");
      }
    }

    /**
     * Get {@link ReplicationRule} from command line
     */
    public static ReplicationRule getRule(CommandLine line)
        throws IllegalArgumentException {
      return ReplicationRule.parseFromString(line.getOptionValue("rule"));
    }

    public static List<Path> getPaths(CommandLine line)
        throws IllegalArgumentException, IOException {
      List<String> rawPaths;
      if (line.hasOption("path")) {
        rawPaths = new ArrayList<>(StringUtils.getTrimmedStringCollection(
            line.getOptionValue("path")));
      } else {
        rawPaths = readPathFile(line.getOptionValue("pathFile"));
      }
      List<Path> paths = new ArrayList<>();
      for (String path: rawPaths) {
        if (!path.startsWith("/")) {
          throw new IllegalArgumentException("Invalid path: " + path);
        }
        paths.add(new Path(path));
      }
      return paths;
    }

    private static Map<String, ReplicationRule> getPathRuleMap(CommandLine line)
        throws IllegalArgumentException, IOException {
      List<String> mapLines = readPathFile(line.getOptionValue("pathRuleFile"));
      Map<String, ReplicationRule> pathRuleMap = new HashMap<>();
      for (String mapLine: mapLines) {
        // the format should be "path rule"
        String[] fields = mapLine.split("\\s", 2);
        if (fields.length != 2) {
          throw new IllegalArgumentException("Invalid map: " + mapLine);
        }
        pathRuleMap.put(fields[0], ReplicationRule.parseFromString(fields[1]));
      }
      return pathRuleMap;
    }

    private static List<String> readPathFile(String file) throws IOException {
      List<String> list = new ArrayList<>();
      BufferedReader reader = new BufferedReader(
          new InputStreamReader(Files.newInputStream(Paths.get(file)), StandardCharsets.UTF_8));
      try {
        String line;
        while ((line = reader.readLine()) != null) {
          if (!line.trim().isEmpty()) {
            list.add(line.trim());
          }
        }
      } finally {
        IOUtils.cleanupWithLogger(LOG, reader);
      }
      return list;
    }

    @Override
    public int run(String[] args) {
      long startTime = Time.monotonicNow();
      final Configuration conf = getConf();
      final Options options = buildCliOptions();
      CommandLineParser parser = new GnuParser();
      final long monitorCheckInterval = conf.getLong(
          DFSConfigKeys.DFS_ZONEMOVER_MONITOR_CHECK_INTERVAL_KEY,
          DFSConfigKeys.DFS_ZONEMOVER_MONITOR_CHECK_INTERVAL_DEFAULT);
      try {
        CommandLine commandLine = parser.parse(options, args, true);
        additionalOptionsCheck(commandLine);
        URI namenode = ZoneUtil.getNamespaceUri(commandLine, conf);
        List<Path> paths;
        ReplicationRule rule = null;
        Map<String, ReplicationRule> pathRuleMap = null;
        if (commandLine.hasOption("pathRuleFile")) {
          pathRuleMap = getPathRuleMap(commandLine);
          paths = ZoneUtil.getPaths(pathRuleMap);
        } else {
          paths = getPaths(commandLine);
          rule = getRule(commandLine);
        }

        if (commandLine.hasOption("monitorByTrigger")) {
          if (rule != null) {
            return ZoneMoverV2.runInMonitor(conf, namenode, paths, rule, null, false);
          } else {
            return ZoneMoverV2.runInMonitor(conf, namenode, paths, null, pathRuleMap, false);
          }
        } else if (commandLine.hasOption("monitor")) {
          //noinspection InfiniteLoopStatement
          while (true) {
            startTime = Time.monotonicNow();
            ZoneMoverV2.runInBatch(conf, namenode, paths, rule, null);
            //noinspection BusyWait
            Thread.sleep(monitorCheckInterval);
          }
        } else {
          if (rule != null) {
            return ZoneMoverV2.runInBatch(conf, namenode, paths, rule, null);
          } else {
            return ZoneMoverV2.runInBatch(conf, namenode, paths, null, pathRuleMap);
          }
        }
      } catch (ParseException | IllegalArgumentException e) {
        System.out.println(e + ".  Exiting ...");
        return ExitStatus.ILLEGAL_ARGUMENTS.getExitCode();
      } catch (InterruptedException e) {
        System.out.println(e + ".  Exiting ...");
        return ExitStatus.INTERRUPTED.getExitCode();
      } catch (Exception e) {
        System.out.println(e + ".  Exiting ...");
        return ExitStatus.IO_EXCEPTION.getExitCode();
      } finally {
        LOG.info("ZoneMover took {}.", (Time.monotonicNow() - startTime));
      }
    }
  }

  public static void main(String[] args) {
    if (DFSUtil.parseHelpArgument(args, Cli.USAGE, System.out, true)) {
      System.exit(0);
    }
    try {
      System.exit(ToolRunner.run(new HdfsConfiguration(), new Cli(), args));
    } catch (Throwable e) {
      LOG.error("Exiting ZoneMover due to an exception", e);
      System.exit(-1);
    }
  }
}