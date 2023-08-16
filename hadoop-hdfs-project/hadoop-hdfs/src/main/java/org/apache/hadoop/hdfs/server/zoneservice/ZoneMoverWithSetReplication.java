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
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.net.DFSNetworkTopologyWithDataCenter;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher;
import org.apache.hadoop.hdfs.server.balancer.ExitStatus;
import org.apache.hadoop.hdfs.server.balancer.NameNodeConnector;
import org.apache.hadoop.hdfs.server.mover.Mover;
import org.apache.hadoop.hdfs.server.zoneservice.metrics.ZoneProgressTracker;
import org.apache.hadoop.hdfs.server.zoneservice.metrics.ZoneServiceMetrics;
import org.apache.hadoop.hdfs.server.zoneservice.store.MigrationRecord;
import org.apache.hadoop.hdfs.server.zoneservice.store.Query;
import org.apache.hadoop.hdfs.server.zoneservice.store.SignalRecord;
import org.apache.hadoop.hdfs.server.zoneservice.store.StoreDriver;
import org.apache.hadoop.hdfs.server.zoneservice.utils.MigrationDataCenters;
import org.apache.hadoop.hdfs.server.zoneservice.utils.RunMode;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ZONESERVICE_STORE_DRIVER_CLASS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ZONESERVICE_STORE_DRIVER_CLASS_DEFAULT;

public class ZoneMoverWithSetReplication extends ZoneMover {
  private static final Logger LOG = LoggerFactory.getLogger(ZoneMoverWithSetReplication.class);
  private static final String ID_PATH_PREFIX = "/system/zoneenhancedmover.id";
  private final boolean allowChangeReplication;

  protected final ProcessorWithSetReplication processor = new ProcessorWithSetReplication();

  private static ReplicaMigrationRuleMap migrationRuleMap;
  private StoreDriver driver;
  private boolean fromZS = false;
  private RunMode runMode = RunMode.BATCH;
  public static final ReplicationRule DEFAULT_RULE =
      ReplicationRule.parseFromString(String.format("%s:2,%s:2,%s:1",
      MigrationDataCenters.STT, MigrationDataCenters.TL, MigrationDataCenters.AT));

  public ZoneMoverWithSetReplication(NameNodeConnector nnc, Configuration conf,
      AtomicInteger retryCount, boolean allowChange) throws IOException {
    super(nnc, conf, retryCount);
    allowChangeReplication = allowChange;
    migrationRuleMap = new ReplicaMigrationRuleMap(conf);
  }

  public ZoneMoverWithSetReplication(NameNodeConnector nnc,
      Configuration conf, ReplicationRule rule,
      AtomicInteger retryCount, boolean allowChange) throws IOException {
    super(nnc, conf, rule, retryCount);
    allowChangeReplication = allowChange;
    migrationRuleMap = new ReplicaMigrationRuleMap(conf);
  }

  public ZoneMoverWithSetReplication(NameNodeConnector nnc,
      Configuration conf, ReplicationRule rule,
      AtomicInteger retryCount, boolean allowChange, boolean fromZS) throws IOException {
    super(nnc, conf, rule, retryCount);
    allowChangeReplication = allowChange;
    migrationRuleMap = new ReplicaMigrationRuleMap(conf);
    this.fromZS = fromZS;
  }

  public ZoneMoverWithSetReplication(NameNodeConnector nnc,
      Configuration conf, Map<String, ReplicationRule> pathRuleMap,
      AtomicInteger retryCount, boolean allowChange, boolean fromZS) throws IOException {
    super(nnc, conf, pathRuleMap, retryCount);
    allowChangeReplication = allowChange;
    migrationRuleMap = new ReplicaMigrationRuleMap(conf);
    this.fromZS = fromZS;
  }

  void intZkDriver(Configuration conf, StoreDriver storeDriver) {
    if (null == storeDriver) {
      Class<? extends StoreDriver> driverClass = conf.getClass(
          DFS_ZONESERVICE_STORE_DRIVER_CLASS,
          DFS_ZONESERVICE_STORE_DRIVER_CLASS_DEFAULT,
          StoreDriver.class);
      this.driver = ReflectionUtils.newInstance(driverClass, conf);
      this.driver.init(conf, "ZoneMoverWithSetReplication");
    } else {
      this.driver = storeDriver;
    }
  }

  void init(RunMode mode, Configuration conf, StoreDriver driver, URI namenode,
      ZoneMoverTrigger zoneMoverTrigger) throws IOException {
    init(conf);

    if (mode == RunMode.MONITOR) {
      intZkDriver(conf, driver);
      createTriggerRuleMapUpdater(namenode, zoneMoverTrigger);
    }
    this.runMode = mode;
  }

  void createTriggerRuleMapUpdater(URI namenode, ZoneMoverTrigger zoneMoverTrigger)
      throws IOException {
    // Create ZoneMoverTriggerRuleUpdater thread to sync the Monitor records in zookeeper.
    String threadName = "Zone-RuleUpdater" + namenode.getAuthority();
    try {
      ZoneMoverTriggerRuleUpdater triggerRuleUpdater = new ZoneMoverTriggerRuleUpdater(namenode,
           zoneMoverTrigger);
      Thread mapUpdaterThread = new Thread(triggerRuleUpdater, threadName);
      mapUpdaterThread.start();
      LOG.info("{} created.", threadName);
    } catch (Exception e) {
      throw new IOException("Failed to create " + threadName, e);
    }
  }

  public static int runWithSetReplication(Configuration conf, URI namenode,
      List<Path> paths)
      throws IOException, InterruptedException {
    return runWithSetReplication(conf, namenode, paths, null);
  }

  public static int runWithSetReplication(Configuration conf, URI namenode,
      List<Path> paths, ReplicationRule rule)
      throws IOException, InterruptedException {
    ZoneProgressTracker.startCountingInitTime();
    if (rule != null) {
      checkDataCenterValues(conf, rule, null);
    } else {
      rule = DEFAULT_RULE;
    }
    LOG.info("Start to apply rule: " + rule + " to namenode:"
        + namenode + ", path: " + paths);
    if (paths.isEmpty()) {
      ZoneProgressTracker.finishCountingInitTimeAndLog();
      return ExitStatus.SUCCESS.getExitCode();
    }

    NameNodeConnector nnc = null;
    ZoneMoverWithSetReplication zm = null;
    // retryCount starts from 0 and ends at retryMaxAttempts
    AtomicInteger retryCount = new AtomicInteger(0);
    final long sleepTime = calculateSleepTime(conf);
    final boolean exitEvenHasProgress = conf.getBoolean(
        DFSConfigKeys.DFS_ZONEMOVER_EXIT_EVEN_HAS_PROGRESS,
        DFSConfigKeys.DFS_ZONEMOVER_EXIT_EVEN_HAS_PROGRESS_DEFAULT);

    try {
      // Set maxNotChangedIterations to 1 as ZoneMoverWithSetReplication does not need to loop
      nnc = new NameNodeConnector(ZoneMoverWithSetReplication.class.getSimpleName(),
          namenode, getIdPath(RunMode.BATCH), paths, conf, 1);
      nnc.getKeyManager().startBlockKeyUpdater();

      zm = new ZoneMoverWithSetReplication(nnc, conf, rule, retryCount, true);
      zm.init(conf);
      int round = 0;

      ZoneProgressTracker.finishCountingInitTimeAndLog();

      while (true) {
        round += 1;
        LOG.info("Start round " + round + " ...");
        final ExitStatus r= zm.run();
        if (r == ExitStatus.SUCCESS) {
          break;
        } else if (r != ExitStatus.IN_PROGRESS) {
          if (r == ExitStatus.NO_MOVE_PROGRESS) {
            System.err.println("Failed to move some blocks after "
                + zm.retryMaxAttempts + " retries. Exiting...");
          } else if (r == ExitStatus.NO_MOVE_BLOCK) {
            System.err.println("Some blocks can't be moved. Exiting...");
          } else {
            System.err.println("ZoneMover failed. Exiting with status " + r + "... ");
          }
          // must be an error statue, return
          return r.getExitCode();
        } else if (exitEvenHasProgress) {
          // If we apply a replication rule to a path with a lot of data,
          // for example, more than 1 PB. Some replicas may encounter
          // moving failure but most succeed. At this case, the ExitStatus will
          // be IN_PROGRESS. It will cost a lot of time to go through all files
          // once more. We can exit here and rerun ZoneMover or not based on the
          // number of failing cases in the log.
          LOG.info("ZoneMover has progress in this round. Exiting ... ");
          return r.getExitCode();
        }
        zm.resetTargetsStatus();
        //noinspection BusyWait
        Thread.sleep(sleepTime);
      }
    } finally {
      if (nnc != null) {
        IOUtils.cleanupWithLogger(LOG, nnc);
      }
      ZoneProgressTracker.checkForLeak();
      if (zm != null) {
        zm.shutdown();
      }
    }

    return ExitStatus.SUCCESS.getExitCode();
  }

  /**
   * Run monitorByTrigger mode from Zone Service.
   */
  public static int run(Configuration conf, URI namenode, StoreDriver driver,
      Map<String, ReplicationRule> replicationRuleMap) throws IOException {
    List<Path> paths = ZoneMover.Cli.getPaths(replicationRuleMap);
    ZoneMoverTrigger zoneMoverTrigger =
        new ZoneMoverKafkaTrigger(conf, paths, namenode);
    return runWithSetReplication(zoneMoverTrigger, conf, namenode, paths, null,
        replicationRuleMap, false, true, driver);
  }

  private static int runWithSetReplication(ZoneMoverTrigger zoneMoverTrigger,
      Configuration conf, URI namenode, List<Path> paths,
      ReplicationRule rule, Map<String, ReplicationRule> pathRuleMap,
      boolean startBatch) throws IOException {
    return runWithSetReplication(zoneMoverTrigger, conf, namenode, paths, rule,
        pathRuleMap, startBatch, false, null);
  }

  private static int runWithSetReplication(ZoneMoverTrigger zoneMoverTrigger,
      Configuration conf, URI namenode, List<Path> paths,
      ReplicationRule rule, Map<String, ReplicationRule> pathRuleMap,
      boolean startBatch, boolean fromZS, StoreDriver driver)
      throws IOException {
    if (startBatch) {
      try {
        if (ExitStatus.SUCCESS.getExitCode() != run(conf, namenode, paths, rule, pathRuleMap)) {
          LOG.error("Move the original data for {} in {} fail.", paths, namenode.getAuthority());
        }
      } catch (InterruptedException e) {
        LOG.error("Batch process is interrupted.", e);
      }
    }
    checkDataCenterValues(conf, rule, pathRuleMap);
    if (paths.isEmpty() && pathRuleMap.isEmpty()) {
      return ExitStatus.SUCCESS.getExitCode();
    }

    // Initialize ZoneService Metrics
    ZoneServiceMetrics zoneServiceMetrics = fromZS ?
        ZoneService.getMetrics() : ZoneServiceMetrics.create();

    NameNodeConnector nnc = null;
    ZoneMoverWithSetReplication zs = null;
    String ns = namenode.getAuthority();
    try {
      LOG.info("Initializing NameNodeConnector");
      nnc = new NameNodeConnector(ZoneMoverWithSetReplication.class.getSimpleName(),
          namenode, getIdPath(RunMode.MONITOR), paths, conf, 1);
      nnc.getKeyManager().startBlockKeyUpdater();
      if (rule != null) {
        zs = new ZoneMoverWithSetReplication(nnc, conf, rule, new AtomicInteger(0),
            true, fromZS);
      } else {
        zs = new ZoneMoverWithSetReplication(nnc, conf, pathRuleMap, new AtomicInteger(0),
            true, fromZS);
      }
      zs.init(RunMode.MONITOR, conf, driver, namenode, zoneMoverTrigger);

      while (zoneMoverTrigger.hasNext()) {
        try {
          String curPath = zoneMoverTrigger.getNext();
          // process the path
          LOG.info("Check path: " + curPath);
          ExitStatus exitStatus = zs.run(curPath);
          if (exitStatus != ExitStatus.SUCCESS) {
            zoneServiceMetrics.incrFailMoveCount();
            zoneServiceMetrics.incrNSMonitorFailMoveCount(ns);
            LOG.warn("Monitor process file fail: " + curPath);
          } else {
            zoneServiceMetrics.incrNSMonitorSuccessMoveCount(ns);
            zoneServiceMetrics.incrSuccessMoveCount();
          }
        } catch (IllegalArgumentException e) {
          LOG.warn(e.toString());
        } catch (InterruptedException e) {
          return ExitStatus.INTERRUPTED.getExitCode();
        }
      }
    } finally {
      if (nnc != null) {
        IOUtils.cleanupWithLogger(LOG, nnc);
      }
      if (zs != null) {
        zs.shutdown();
      }
      if (zoneMoverTrigger != null) {
        zoneMoverTrigger.shutdown();
      }
    }
    return ExitStatus.SUCCESS.getExitCode();
  }

  public static int checkWithSetReplication(Configuration conf, URI namenode, List<Path> paths,
      MigrationDataCenters dc) throws IOException, InterruptedException {
    LOG.info("Start to check path {} if it has replica in dc {} or not", paths, dc);
    if (paths.isEmpty()) {
      return ExitStatus.SUCCESS.getExitCode();
    }

    ZoneProgressTracker.startCountingInitTime();
    NameNodeConnector nnc = null;
    ZoneMoverWithSetReplication zm = null;
    // retryCount starts from 0 and ends at retryMaxAttempts
    AtomicInteger retryCount = new AtomicInteger(0);
    final long sleepTime = calculateSleepTime(conf);
    final boolean exitEvenHasProgress = conf.getBoolean(
        DFSConfigKeys.DFS_ZONEMOVER_EXIT_EVEN_HAS_PROGRESS,
        DFSConfigKeys.DFS_ZONEMOVER_EXIT_EVEN_HAS_PROGRESS_DEFAULT);

    try {
      // Set maxNotChangedIterations to 1 as ZoneMoverWithSetReplication does not need to loop
      nnc = new NameNodeConnector(ZoneMoverWithSetReplication.class.getSimpleName(),
          namenode, getIdPath(RunMode.CHECK), paths, conf, 1);
      nnc.getKeyManager().startBlockKeyUpdater();

      zm = new ZoneMoverWithSetReplication(nnc, conf, DEFAULT_RULE, retryCount, true);
      zm.init(RunMode.CHECK, conf, null, null, null);
      int round = 0;

      ZoneProgressTracker.finishCountingInitTimeAndLog();

      while (true) {
        round += 1;
        LOG.info("Start round " + round + " ...");
        final ExitStatus r= zm.run(dc);
        if (r == ExitStatus.SUCCESS) {
          break;
        } else if (r != ExitStatus.IN_PROGRESS) {
          if (r == ExitStatus.NO_MOVE_PROGRESS) {
            System.err.println("Failed to move some blocks after "
                + zm.retryMaxAttempts + " retries. Exiting...");
          } else if (r == ExitStatus.NO_MOVE_BLOCK) {
            System.err.println("Some blocks can't be moved. Exiting...");
          } else {
            System.err.println("ZoneMover failed. Exiting with status " + r + "... ");
          }
          // must be an error statue, return
          return r.getExitCode();
        } else if (exitEvenHasProgress) {
          // If we apply a replication rule to a path with a lot of data,
          // for example, more than 1 PB. Some replicas may encounter
          // moving failure but most succeed. At this case, the ExitStatus will
          // be IN_PROGRESS. It will cost a lot of time to go through all files
          // once more. We can exit here and rerun ZoneMover or not based on the
          // number of failing cases in the log.
          LOG.info("ZoneMover has progress in this round. Exiting ... ");
          return r.getExitCode();
        }
        zm.resetTargetsStatus();
        //noinspection BusyWait
        Thread.sleep(sleepTime);
      }
    } finally {
      if (nnc != null) {
        IOUtils.cleanupWithLogger(LOG, nnc);
      }
      if (zm != null) {
        zm.shutdown();
      }
    }

    return ExitStatus.SUCCESS.getExitCode();
  }

  @Override
  ExitStatus run() {
    try {
      return new ProcessorWithSetReplication().processPath().getExitStatus();
    } catch (IllegalArgumentException e) {
      System.out.println(e + ".  Exiting ...");
      return ExitStatus.ILLEGAL_ARGUMENTS;
    }
  }

  ExitStatus run(String path) throws IllegalArgumentException {
    Mover.Result result = new Mover.Result();
    this.result = result;

    // Filters path rules from "ZoneService" for ZoneMover.
    if (!fromZS && getFilteredPathRulesFromZS(path) != null) {
      LOG.info("ZoneMover will filter the monitor path {} from zone service.", path);
      return result.getExitStatus();
    }

    if (this.globalRule != null) {
      processor.processPath(path, this.globalRule, result, null);
    } else {
      processor.processPath(path, getPathRule(path), result, null);
    }
    return result.getExitStatus();
  }

  ExitStatus run(MigrationDataCenters dc) {
    try {
      return new ProcessorWithSetReplication().processPath(dc).getExitStatus();
    } catch (IllegalArgumentException e) {
      System.out.println(e + ".  Exiting ...");
      return ExitStatus.ILLEGAL_ARGUMENTS;
    }
  }

  static Path getIdPath(RunMode mode) {
    return new Path(String.format("%s.%s.%s.%s",
        ID_PATH_PREFIX,
        mode.toString().toLowerCase(),
        NetUtils.getLocalHostname(),
        Time.now()));
  }

  class ProcessorWithSetReplication {
    private Mover.Result processPath() {
      return processPath(null);
    }

    private Mover.Result processPath(MigrationDataCenters dc) {
      ZoneProgressTracker.resetTracker();
      ZoneProgressTracker.trackPaths(dispatcher.getDistributedFileSystem(), targetPaths);
      ZoneProgressTracker.startCountingProcessPathTime();

      result = new Mover.Result();
      for (Path target: targetPaths) {
        if (globalRule != null) {
          processPath(target.toUri().getPath(), globalRule, result, dc);
        } else {
          String path = target.toUri().getPath();
          processor.processPath(path, getPathRule(path), result, dc);
        }
      }
      ZoneProgressTracker.finishCountingProcessPathTimeAndLog();

      coordinator.waitForCheckCompletion();

      ZoneProgressTracker.startCountingFetcherWaitTime();
      try {
        fetcher.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        ZoneProgressTracker.finishCountingFetcherWaitTimeAndLog();
      }

      // wait for pending move to finish and retry the failed migration
      ZoneProgressTracker.startCountingMoveCompletionWaitTime();
      boolean hasFailed = Dispatcher.waitForMoveCompletion(storages.targets.values());
      ZoneProgressTracker.finishCountingMoveCompletionWaitTimeAndLog();

      ZoneProgressTracker.startCountingPostProcessingTime();
      boolean hasSuccess = Dispatcher.checkForSuccess(storages.targets.values());
      // check and update retryCount
      if (hasFailed && !hasSuccess) {
        if (retryCount.get() == retryMaxAttempts) {
          result.setRetryFailed();
          LOG.error("Failed to move some block's after "
              + retryMaxAttempts + " retries.");
          ZoneProgressTracker.finishCountingPostProcessingTimeAndLog();
          return result;
        } else {
          retryCount.incrementAndGet();
        }
      } else {
        // Reset retry count if no failure or have success.
        retryCount.set(0);
      }

      if (hasFailed) {
        result.updateHasRemaining(true);
      }
      ZoneProgressTracker.finishCountingPostProcessingTimeAndLog();
      return result;
    }

    private void processPath(String fullPath, ReplicationRule rule, Mover.Result result,
        MigrationDataCenters dc) {
      LOG.info("Processing path: {}, mode: {}", fullPath, runMode);
      for (byte[] lastReturnedName = HdfsFileStatus.EMPTY_NAME;;) {
        final DirectoryListing children;
        try {
          dfs.msync();
          children = dfs.listPaths(fullPath, lastReturnedName, true);
        } catch(IOException e) {
          LOG.warn("Failed to list directory " + fullPath
              + ". Ignore the directory and continue.", e);
          return;
        }
        if (children == null) {
          return;
        }
        for (HdfsFileStatus child : children.getPartialListing()) {
          processRecursively(fullPath, child, rule, result, dc);
        }
        if (children.hasMore()) {
          lastReturnedName = children.getLastName();
        } else {
          return;
        }
      }
    }

    private void processRecursively(String parent, HdfsFileStatus status, ReplicationRule rule,
        Mover.Result result, MigrationDataCenters dc) {
      String fullPath = status.getFullName(parent);
      if (status.isDir()) {
        if (!fullPath.endsWith(Path.SEPARATOR)) {
          fullPath = fullPath + Path.SEPARATOR;
        }
        processPath(fullPath, rule, result, dc);
      } else if (!status.isSymlink()) { // file
        processFile(fullPath, (HdfsLocatedFileStatus) status, rule, result, dc);
      }
    }

    private void processFile(String fullPath,
        HdfsLocatedFileStatus status, ReplicationRule rule,
        Mover.Result result, MigrationDataCenters dc) {

      if (status.getErasureCodingPolicy() != null) {
        LOG.info("Processing EC file: " + fullPath + " ....");
        // Tracker is updated inside the method call so no need to incr tracker file count here
        processECFile(fullPath, status, result);
        return;
      }

      LOG.info("Processing file: {}, mode: {}", fullPath, runMode);

      final LocatedBlocks locatedBlocks = status.getBlockLocations();
      if (status.getLen() == 0) {
        LOG.info("Skip empty file: " + fullPath);
        ZoneProgressTracker.incrFileCount();
        return;
      }

      if (!locatedBlocks.isLastBlockComplete()) {
        LOG.info("Skip uncompleted file: " + fullPath);
        ZoneProgressTracker.incrFileCount();
        return;
      }

      ReplicationRule appliedRule;

      if (allowChangeReplication) {
        LocatedBlock firstBlock = status.getBlockLocations().get(0);
        Map<String, Short> blockDistribution = getBlockDistribution(firstBlock);
        if(blockDistribution.size() == 0) {
          LOG.error("There are no replicas for the missing block {}", firstBlock);
          return;
        }
        ReplicationRule dis = ReplicationRule.parseFromMap(blockDistribution);
        if (runMode == RunMode.BATCH) {
          appliedRule = migrationRuleMap.getRuleFromDistribution(dis, status.getReplication());
        } else if (runMode == RunMode.CHECK) {
          if (dc == null) {
            LOG.warn("Using check mode but not give the data center!");
            ZoneProgressTracker.incrFileCount();
            return;
          }
          appliedRule = migrationRuleMap.checkDistribution(dis, status.getReplication(), dc);
          if (appliedRule == null) {
            ZoneProgressTracker.incrFileCount();
            return;
          }
        } else {
          appliedRule =
              migrationRuleMap.generateRule(rule, status.getReplication(), dis);
          if (appliedRule == null) {
            appliedRule = migrationRuleMap.getDefaultRule(dis, status.getReplication());
          }
        }

        LOG.info("Will apply the rule from {} to {} with replication {} on {}", dis, appliedRule,
            appliedRule.getReplica(), fullPath);
        try {
          if (appliedRule.getReplica() != status.getReplication()) {
            dfs.setReplication(fullPath, appliedRule.getReplica());
          }
        } catch (IOException e) {
          LOG.warn("Set replication fails for {}\n {}", fullPath, e);
          result.setRetryFailed();
        }
      } else {
        LOG.warn("Ignore replica not consistent file: {}", fullPath);
        ZoneProgressTracker.incrFileCount();
        return;
      }

      if (xattrSetEnable) {
        boolean needSet = true;
        try {
          if (ruleUtil.hasRuleInXAttr(fullPath)) {
            try {
              if (ruleUtil.getRuleFromXAttr(fullPath).equals(appliedRule)) {
                LOG.info("This file already has the replicationRule: " + fullPath);
                needSet = false;
              }
            } catch (IllegalArgumentException e) {
              LOG.warn("Found an invalid rule in NameNode, will update it: file={}, content={}",
                  fullPath, ruleUtil.getStringFromRuleKey(fullPath));
            }
          }

          if (needSet) {
            ruleUtil.setRuleToXAttr(fullPath, appliedRule);
            LOG.info("Added replicationRule to: " + fullPath);
          }
        } catch (IOException e) {
          LOG.warn(e.toString());
          ZoneProgressTracker.incrFileCount();
          return;
        }
      }


      if (status.getReplication() < appliedRule.getReplica()) {
        LOG.debug("factor < rule.replication file: " + fullPath);
        // For cases like changing "/Telin-3:3" to "/Telin-3:3,/Telin-4:2",
        // ZoneMoverWithSetReplication does not need to wait for NameNode to schedule replication.
        // For cases like changing "/Telin-3:3" to "/Telin-3:2,/Telin-4:3",
        // ZoneMoverWithSetReplication needs to wait for NameNode to schedule first and then
        // do redistribution. If ZoneMoverWithSetReplication does not wait, it will increase
        // the number of inter-dc block replications.
        if (isFileNeedMove(locatedBlocks, appliedRule) || allowChangeReplication) {
          coordinator.addFile(fullPath, appliedRule,
              appliedRule.getReplica() - status.getReplication(),
              locatedBlocks.getLocatedBlocks().size());
        }
      } else if (status.getReplication() > appliedRule.getReplica()) {
        LOG.debug("factor > rule.replication file: " + fullPath);
        // For cases like changing "/Telin-3:3,/Telin-4:2" -> "/Telin-3:3",
        // ZoneMoverWithSetReplication does not need to wait.
        // For cases like changing "/Telin-3:3" to "/Telin-3:1,/Telin-4:1",
        // ZoneMoverWithSetReplication needs to wait. Otherwise, NameNode may schedule the
        // datanode to delete the replica first, which is also used as the
        // proxy of replaceBlock scheduled by ZoneMoverWithSetReplication.
        if (isFileNeedMove(locatedBlocks, appliedRule) || allowChangeReplication) {
          coordinator.addFile(fullPath, appliedRule,
              appliedRule.getReplica() - status.getReplication(),
              locatedBlocks.getLocatedBlocks().size());
        }
      } else {
        processFileBlocks(fullPath, status, appliedRule, result);
      }
    }

    /**
     * Process EC file, move all the TL replicas to STT.
     * */
    private void processECFile(String fullPath, HdfsLocatedFileStatus status, Mover.Result result) {
      final LocatedBlocks locatedBlocks = status.getBlockLocations();
      final ErasureCodingPolicy erasureCodingPolicy = status.getErasureCodingPolicy();
      String TLname = MigrationDataCenters.TL.getName();
      int n = locatedBlocks.locatedBlockCount();
      for (int i=0; i<n; i++) {
        List<ZoneMoveItem> moveItems = new ArrayList<>();
        LocatedBlock block = locatedBlocks.get(i);
        Map<String, Short> distribution = getBlockDistribution(block);
        if (distribution.containsKey(TLname)) {
          moveItems.add(new ZoneMoveItem(TLname, MigrationDataCenters.STT.getName(),
              distribution.get(TLname)));
          if (scheduleMoves4Block(fullPath, block, moveItems, erasureCodingPolicy)) {
            result.setNoBlockMoved(false);
          } else {
            result.updateHasRemaining(true);
          }
        }
      }
    }

    private boolean isFileNeedMove(
        LocatedBlocks locatedBlocks, ReplicationRule rule) {
      for (LocatedBlock block: locatedBlocks.getLocatedBlocks()) {
        if (!getZoneMoveItems(block, rule).isEmpty()) {
          return true;
        }
      }
      return false;
    }

    private void processFileBlocks(String fullPath,
        HdfsLocatedFileStatus status, ReplicationRule rule, Mover.Result result) {
      final LocatedBlocks locatedBlocks = status.getBlockLocations();
      // Cannot just check the first and last block, as the dispatching action
      // is parallel and asynchronous. The movement of any blocks of a file
      // may fail but other blocks succeed.
      if (areBlocksDistributionConsistent(locatedBlocks.getLocatedBlocks())) {
        // get the first block
        LocatedBlock firstBlock = locatedBlocks.get(0);
        if (isBlockSatisfyRule(firstBlock, rule)) {
          LOG.info("Skip the file as all blocks already satisfy the rule: " + fullPath);
          ZoneProgressTracker.incrFileCount();
          return;
        }
        processConsistentBlocks(fullPath, locatedBlocks, rule, result);
      } else {
        processInconsistentBlocks(fullPath, locatedBlocks, rule, result);
      }
    }

    /**
     * Process blocks have the same datacenter distribution.
     */
    private void processConsistentBlocks(String fullPath,
        LocatedBlocks locatedBlocks, ReplicationRule rule, Mover.Result result) {
      LocatedBlock firstBlock = locatedBlocks.get(0);
      if (isBlockSatisfyRule(firstBlock, rule)) {
        ZoneProgressTracker.incrFileCount();
        return;
      }
      List<ZoneMoveItem> moveItems = getZoneMoveItems(firstBlock, rule);
      int n = locatedBlocks.locatedBlockCount();

      // Do a dummy queue here to ensure the last dequeue of this path
      // is either the last dispatch executed or the end of this method, whichever happens later
      ZoneProgressTracker.queueFile(fullPath);

      for (int i=0; i<n; i++) {
        LocatedBlock block = locatedBlocks.get(i);
        if (scheduleMoves4Block(fullPath, block, moveItems)) {
          result.setNoBlockMoved(false);
        } else {
          result.updateHasRemaining(true);
        }
      }
      ZoneProgressTracker.dequeueFile(fullPath);
    }

    /**
     * Process blocks have different datacenter distributions.
     */
    private void processInconsistentBlocks(String fullPath,
        LocatedBlocks locatedBlocks, ReplicationRule rule, Mover.Result result) {
      int n = locatedBlocks.locatedBlockCount();

      // Do a dummy queue here to ensure the last dequeue of this path
      // is either the last dispatch executed or the end of this method, whichever happens later
      ZoneProgressTracker.queueFile(fullPath);

      for (int i=0; i<n; i++) {
        LocatedBlock block = locatedBlocks.get(i);
        if (isBlockSatisfyRule(block, rule)) {
          continue;
        }
        if (scheduleMoves4Block(fullPath, block, getZoneMoveItems(block, rule))) {
          result.setNoBlockMoved(false);
        } else {
          result.updateHasRemaining(true);
        }
      }
      ZoneProgressTracker.dequeueFile(fullPath);
    }

    boolean scheduleMoves4Block(String fullPath, LocatedBlock lb, List<ZoneMoveItem> moveItems) {
      return scheduleMoves4Block(fullPath, lb, moveItems, null);
    }


    boolean scheduleMoves4Block(String fullPath, LocatedBlock lb, List<ZoneMoveItem> moveItems,
        ErasureCodingPolicy ecPolicy) {
      final List<Mover.MLocation> locations = Mover.MLocation.toLocations(lb);
      // put locations to a map with datacenter as the key
      final Map<String, List<Mover.MLocation>> locationMap = new HashMap<>();
      for (Mover.MLocation ml : locations) {
        String dc = DFSNetworkTopologyWithDataCenter.getDataCenter(ml.getDatanode().getNetworkLocation());
        if (locationMap.containsKey(dc)) {
          locationMap.get(dc).add(ml);
        } else {
          locationMap.put(dc, new ArrayList<>(Collections.singletonList(ml)));
        }
      }

      final Dispatcher.DBlock db = newDBlock(lb, locations, ecPolicy);
      Set<StorageType> targetTypes = new HashSet<>(Arrays.asList(lb.getStorageTypes()));
      Set<Dispatcher.DDatanode.StorageGroup> excluded = getExcluded(locations, moveItems);
      // get MLocation according to datacenter and select source
      for (ZoneMoveItem moveItem : moveItems) {
        for (short i = 0; i < moveItem.getNum(); i++) {
          List<Mover.MLocation> sourceLocations = locationMap.get(moveItem.getSourceDataCenter());
          Mover.MLocation location = sourceLocations.get(0);
          sourceLocations.remove(0);
          ZoneDispatcher.ZoneSource source = storages.getSource(location);
          if (source != null) {
            if (!scheduleMoveReplica(fullPath, db, source, moveItem.getTargetDataCenter(),
                targetTypes, excluded)) {
              return false;
            }
          } else {
            LOG.warn("Failed to get a source for : " + location + ", will skip this replica");
          }
        }
      }
      return true;
    }

    Dispatcher.DBlock newDBlock(LocatedBlock lb, List<Mover.MLocation> locations,
        ErasureCodingPolicy ecPolicy) {
      Block blk = lb.getBlock().getLocalBlock();
      Dispatcher.DBlock db;
      if (lb.isStriped()) {
        LocatedStripedBlock lsb = (LocatedStripedBlock) lb;
        byte[] indices = new byte[lsb.getBlockIndices().length];
        for (int i = 0; i < indices.length; i++) {
          indices[i] = (byte) lsb.getBlockIndices()[i];
        }
        db = new Dispatcher.DBlockStriped(blk, indices, (short) ecPolicy.getNumDataUnits(),
            ecPolicy.getCellSize());
      } else {
        db = new Dispatcher.DBlock(blk);
      }
      for(Mover.MLocation ml : locations) {
        Dispatcher.DDatanode.StorageGroup source = storages.getSource(ml);
        if (source != null) {
          db.addLocation(source);
        }
      }
      return db;
    }

    private Set<Dispatcher.DDatanode.StorageGroup> getExcluded(
        List<Mover.MLocation> locations, List<ZoneMoveItem> moveItems) {
      Set<String> targetDataCenters = new HashSet<>();
      for (ZoneMoveItem item: moveItems) {
        targetDataCenters.add(item.targetDataCenter);
      }

      Set<Dispatcher.DDatanode.StorageGroup> excluded = new HashSet<>();
      for (Mover.MLocation ml: locations) {
        String dc = DFSNetworkTopologyWithDataCenter.getDataCenter(
            ml.getDatanode().getNetworkLocation());
        if (targetDataCenters.contains(dc)) {
          excluded.add(storages.getTarget(ml));
        }
      }
      return excluded;
    }

    boolean scheduleMoveReplica(String fullPath, Dispatcher.DBlock db, ZoneDispatcher.ZoneSource source, String targetDataCenter,
        Set<StorageType> targetTypes, Set<Dispatcher.DDatanode.StorageGroup> excluded) {
      return chooseTargetInDataCenter(fullPath,
          db, source, targetDataCenter, targetTypes, excluded);
    }

    /**
     * Choose a storage in the datacenter.
     */
    boolean chooseTargetInDataCenter(String fullPath,
        Dispatcher.DBlock db, ZoneDispatcher.ZoneSource source, String targetDataCenter,
        Set<StorageType> targetTypes, Set<Dispatcher.DDatanode.StorageGroup> excluded) {
      for (StorageType t: targetTypes) {
        final List<Dispatcher.DDatanode.StorageGroup> targets = storages.getTargetStorages(t, targetDataCenter);
        Collections.shuffle(targets);
        for (Dispatcher.DDatanode.StorageGroup target: targets) {
          if (excluded.contains(target)) {
            continue;
          }
          final Dispatcher.PendingMove pm = source.addPendingMove(fullPath, db, target);
          if (pm != null) {
            dispatcher.executePendingMove(pm);
            excluded.add(target);
            return true;
          }
        }
      }
      LOG.warn("chooseTargetInDataCenter failed with block: " +
          db + ", source: " + source + ", targetDataCenter: " + targetDataCenter +
          ",targetTypes: " + targetTypes + ", excluded: " + excluded);
      handleChooseFail(targetDataCenter, targetTypes);
      return false;
    }

    void handleChooseFail(String targetDataCenter, Set<StorageType> targetTypes) {
      for (StorageType t: targetTypes) {
        final List<Dispatcher.DDatanode.StorageGroup> targets = storages.getTargetStorages(t, targetDataCenter);
        int total = 0;
        for (Dispatcher.DDatanode.StorageGroup target: targets) {
          total += target.getDDatanode().getPendingSize();
        }
        if (targets.size() > 0) {
          LOG.info("Average pending size for targetDataCenter: " + targetDataCenter +
              ", storageType: " + t + ", datanodes.num: "+ targets.size() +
              " is " + total / targets.size());
        } else {
          LOG.warn("targets.size = 0 !");
        }
      }
      try {
        Thread.sleep(DELAY_AFTER_CHOOSE_FAIL);
      } catch (InterruptedException e) {
        // ignore
      }
    }
  }

  static class Cli extends Configured implements Tool {
    private static final String USAGE = "Usage: hdfs zoneenhancedmover"
        + "\n\t[-namespace <namespace>]\tthe namespace to apply the rule"
        + "\n\t-path <path>\tthe path to apply the rule"
        + "\n\t-pathFile <pathFile>\t the file contains paths to apply the rule"
        + "\n\t-rule <rule>\tthe replication rule"
        + "\n\t-pathRuleFile <pathRuleFile> the file contains path and rule mappings."
        + "\n\t               Path and rule are separated by space in each line."
        + "\n\t-monitor\tenable monitor mode"
        + "\n\t-monitorByTrigger\tenable monitor mode with trigger"
        + "\n\t-startBatch\tenable batch mode before monitor mode"
        + "\n\t-dc\tenable check mode to check if the files"
        + "\n\t               under the given directory has replica in specific DC";

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

      option = new Option(null, "startBatch", false, "enable batch mode before monitor");
      options.addOption(option);

      option = new Option(null, "dc", true, "DataCenter will be checked");
      options.addOption(option);
      return options;
    }

    private static void additionalOptionsCheck(CommandLine line)
        throws IllegalArgumentException {
      // As commons-cli 1.2 does not support adding one option
      // to two OptionGroups, we need to check this in a trick way.
      if (((line.hasOption("rule") && line.hasOption("pathRuleFile")) ||
          (!line.hasOption("rule") && !line.hasOption("pathRuleFile")) &&
              (line.hasOption("monitor") || line.hasOption("monitorByTrigger")))) {
        throw new IllegalArgumentException
            ("'-rule' and '-pathRuleFile' CAN and MUST specify one");
      }
    }

    /**
     * Get the URI of the specified namespace
     */
    private static URI getNamespaceUri(CommandLine line, Configuration conf)
        throws IllegalArgumentException {
      Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
      if (!line.hasOption("namespace")) {
        if (namenodes.size() > 1) {
          throw new IllegalArgumentException(
              "Namespace must be specified in a federation cluster!");
        } else {
          return namenodes.iterator().next();
        }
      }

      String namespace = line.getOptionValue("namespace");
      for (URI namenode: namenodes) {
        LOG.info("Get namenode: " + namenode);
        if (namenode.getAuthority().equals(namespace)) {
          return namenode;
        }
      }
      throw new IllegalArgumentException(
          "Cannot find the NameNode for namespace: " + namespace);
    }

    /**
     * Get {@link ReplicationRule} from command line
     */
    private static ReplicationRule getRule(CommandLine line)
        throws IllegalArgumentException {
      return ReplicationRule.parseFromString(line.getOptionValue("rule"));
    }

    private static List<Path> getPaths(CommandLine line)
        throws IllegalArgumentException, IOException {
      List<String> rawPaths;
      if (line.hasOption("path")) {
        rawPaths = new ArrayList<>(Collections.singletonList(line.getOptionValue("path")));
      } else {
        rawPaths = readPathFile(line.getOptionValue("pathFile"));
      }
      List<Path> paths = new ArrayList<>();
      for (String path: rawPaths) {
        if (!path.startsWith(ROOT)) {
          throw new IllegalArgumentException("Invalid path: " + path);
        }
        paths.add(new Path(path));
      }
      return paths;
    }

    public static List<Path> getPaths(Map<String, ReplicationRule> pathRuleMap) {
      Set<String> stringPaths = pathRuleMap.keySet();
      List<Path> paths = new ArrayList<>();
      for (String path: stringPaths) {
        if (!path.startsWith(ROOT)) {
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

    private static MigrationDataCenters getDC(CommandLine line) {
      if (!line.hasOption("dc")) {
        return null;
      }
      String dc = line.getOptionValue("dc");
      return MigrationDataCenters.valueOf(dc);
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
        checkReplicationPolicyCompatibility(conf);
        CommandLine commandLine = parser.parse(options, args, true);
        additionalOptionsCheck(commandLine);
        URI namenode = getNamespaceUri(commandLine, conf);
        List<Path> paths;
        ReplicationRule rule = null;
        Map<String, ReplicationRule> pathRuleMap = null;
        if (commandLine.hasOption("pathRuleFile")) {
          pathRuleMap = getPathRuleMap(commandLine);
          paths = getPaths(pathRuleMap);
        } else if (commandLine.hasOption("rule")){
          paths = getPaths(commandLine);
          rule = getRule(commandLine);
        } else {
          paths = getPaths(commandLine);
        }

        if (commandLine.hasOption("dc")) {
          return check(conf, namenode, paths, getDC(commandLine));
        } else if (commandLine.hasOption("monitorByTrigger")) {
          ZoneMoverTrigger zoneMoverTrigger =
              new ZoneMoverKafkaTrigger(conf, paths, namenode);
          boolean startBatch = commandLine.hasOption("startBatch");
          if (rule != null) {
            return run(zoneMoverTrigger, conf, namenode, paths, rule, startBatch);
          } else {
            return run(zoneMoverTrigger, conf, namenode, paths, pathRuleMap, startBatch);
          }
        } else if (commandLine.hasOption("monitor")) {
          //noinspection InfiniteLoopStatement
          while (true) {
            startTime = Time.monotonicNow();
            run(conf, namenode, paths);
            LOG.info("ZoneMoverWithSetReplication took "
                + StringUtils.formatTime(Time.monotonicNow() - startTime));
            //noinspection BusyWait
            Thread.sleep(monitorCheckInterval);
          }
        } else {
          return run(conf, namenode, paths);
        }
      } catch (IOException e) {
        System.out.println(e + ".  Exiting ...");
        return ExitStatus.IO_EXCEPTION.getExitCode();
      } catch (ParseException | IllegalArgumentException e) {
        System.out.println(e + ".  Exiting ...");
        return ExitStatus.ILLEGAL_ARGUMENTS.getExitCode();
      } catch (InterruptedException e) {
        System.out.println(e + ".  Exiting ...");
        return ExitStatus.INTERRUPTED.getExitCode();
      } finally {
        LOG.info("ZoneMoverWithSetReplication took "
            + StringUtils.formatTime(Time.monotonicNow() - startTime));
      }
    }

    int check(Configuration conf, URI namenode,
        List<Path> paths, MigrationDataCenters dc)
        throws IOException, InterruptedException {
      return ZoneMoverWithSetReplication.checkWithSetReplication(conf, namenode, paths, dc);
    }

    /**
     * Run with prepared arguments.
     */
    int run(Configuration conf, URI namenode,
        List<Path> paths)
        throws IOException, InterruptedException {
      return ZoneMoverWithSetReplication.runWithSetReplication(conf, namenode, paths);
    }

    /**
     * Run with ZoneMoverTrigger for monitorByTrigger mode
     */
    int run(ZoneMoverTrigger zoneMoverTrigger, Configuration conf,
        URI namenode, List<Path> paths, ReplicationRule rule, boolean startBatch)
        throws IOException {
      return ZoneMoverWithSetReplication.runWithSetReplication(
          zoneMoverTrigger, conf, namenode, paths, rule,
          new HashMap<String, ReplicationRule>(), startBatch);
    }

    /**
     * Run with ZoneMoverTrigger for monitorByTrigger mode
     */
    int run(ZoneMoverTrigger zoneMoverTrigger, Configuration conf,
        URI namenode, List<Path> paths, Map<String, ReplicationRule> pathRuleMap,
        boolean startBatch)
        throws IOException, InterruptedException {
      return ZoneMoverWithSetReplication.runWithSetReplication(
          zoneMoverTrigger, conf, namenode, paths, null, pathRuleMap, startBatch);
    }
  }

  /* Monitor records in zookeeper and sync the records */
  class ZoneMoverTriggerRuleUpdater implements Runnable {
    private final URI namenode;
    private final ZoneMoverTrigger zoneMoverTrigger;

    public ZoneMoverTriggerRuleUpdater(URI namenode, ZoneMoverTrigger zoneMoverTrigger) {
      this.namenode = namenode;
      this.zoneMoverTrigger = zoneMoverTrigger;
    }

    @Override
    public void run() {
      LOG.info("Monitor path rule map process start.");
      while (true) {
        try {
          if (fromZS) {
            // For Zone Service: Update records to the pathRuleMap.
            updatePathRuleForZoneService();
          } else {
            // For Zone Mover: Update records to the filteredPathRuleMap.
            updatePathRuleForZoneMover();
          }
          // Wait for the specified interval before continuing execution.
          Thread.sleep(checkUpdateInterval * 1000L);
        } catch (IOException e) {
          LOG.error("There are some errors happen when ZoneMover updates path-rule pairs.", e);
        } catch (InterruptedException e) {
          LOG.warn("Monitor path rule map process is interrupted!");
          break;
        }
      }
    }

    private void updatePathRuleForZoneService() throws IOException {
      SignalRecord signalRecord = new SignalRecord(namenode.getAuthority());
      SignalRecord existingSignalRecord = driver.get(new Query<>(signalRecord), SignalRecord.class);
      if (existingSignalRecord != null && existingSignalRecord.isNeedUpdate()) {
        updatePathRuleMap(driver, namenode.getAuthority(), zoneMoverTrigger);
        existingSignalRecord.finishUpdate();
        driver.put(existingSignalRecord, true, false);
      }
    }

    private void updatePathRuleForZoneMover() throws IOException {
      updatePathRuleMap(driver, namenode.getAuthority(), zoneMoverTrigger);
    }

    private void updatePathRuleMap(StoreDriver driver, String nameSpace,
        ZoneMoverTrigger zoneMoverTrigger) throws IOException {

      Map<String, ReplicationRule> pathRuleMapTmp = new HashMap<>();
      Map<String, ReplicationRule> filterPathRuleMapTmp = new HashMap<>();

      List<MigrationRecord> records =
          driver.getAll(MigrationRecord.class).getRecords();
      for (MigrationRecord record : records) {
        // Process only records in "monitor" mode and the specified name space.
        if (record.getMode().equals(RunMode.MONITOR.getName()) &&
            record.getNs().equals(nameSpace)) {
          String rule = record.getRule();
          if (StringUtils.isNullOrEmpty(rule)) {
            LOG.warn("Failed adding record: {} , due replication rule as null will skip.", record);
            continue;
          }
          ReplicationRule parsedRule = ReplicationRule.parseFromString(record.getRule());
          if (fromZS) {
            // For Zone Service: Add records to the pathRuleMap.
            LOG.debug("Adding record: {} to the pathRuleMap for Zone Service.", record);
            pathRuleMapTmp.put(record.getPath(), parsedRule);
          } else {
            // For Zone Mover: Add records to the filteredPathRuleMap.
            LOG.debug("Adding record: {} to the filteredPathRuleMap for Zone Mover.", record);
            filterPathRuleMapTmp.put(record.getPath(), parsedRule);
          }
        }
      }

      if (fromZS) {
        // Update the pathRuleMap and monitorPaths for Zone Service.
        pathRuleMap.clear();
        pathRuleMap = pathRuleMapTmp;
        zoneMoverTrigger.updatePaths(ZoneMover.Cli.getPaths(pathRuleMap));
      } else {
        // Update the filteredPathRulesFromZS for Zone Mover.
        filteredPathRulesFromZS.clear();
        filteredPathRulesFromZS = filterPathRuleMapTmp;
      }
    }
  }

  public static void main(String[] args) {
    if (DFSUtil.parseHelpArgument(args, ZoneMoverWithSetReplication.Cli.USAGE, System.out, true)) {
      System.exit(0);
    }

    try {
      System.exit(ToolRunner.run(new HdfsConfiguration(), new ZoneMoverWithSetReplication.Cli(), args));
    } catch (Throwable e) {
      LOG.error("Exiting ZoneMoverWithSetReplication due to an exception", e);
      System.exit(-1);
    }
  }
}
