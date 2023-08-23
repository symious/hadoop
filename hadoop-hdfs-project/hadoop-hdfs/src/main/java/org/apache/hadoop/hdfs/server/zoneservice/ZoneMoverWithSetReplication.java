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
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.Block;
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ZONESERVICE_STORE_DRIVER_CLASS;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ZONESERVICE_STORE_DRIVER_CLASS_DEFAULT;

public class ZoneMoverWithSetReplication extends ZoneMover {
  private static final Logger LOG = LoggerFactory.getLogger(ZoneMoverWithSetReplication.class);
  private static final String ID_PATH_PREFIX = "/system/zoneenhancedmover.id";
  private boolean allowChangeReplication;

  private static ReplicaMigrationRuleMap migrationRuleMap;
  private StoreDriver driver;
  private boolean fromZS = false;
  private RunMode runMode = RunMode.BATCH;
  private BlockingQueue<PreMigrationFile> preMigrationFileQueue;
  private long preMigrationCheckInterval;
  private CountDownLatch preMigrationLatch;
  protected final Thread
      preMigrationChecker = new Thread(new PreMigrationChecker(), "ZoneMover-PreMigrationChecker");
  public static final ReplicationRule DEFAULT_RULE =
      ReplicationRule.parseFromString(String.format("%s:2,%s:2,%s:1",
      MigrationDataCenters.STT, MigrationDataCenters.TL, MigrationDataCenters.AT));

  public ZoneMoverWithSetReplication(NameNodeConnector nnc, Configuration conf,
      AtomicInteger retryCount, boolean allowChange) throws IOException {
    super(nnc, conf, retryCount);
    initZoneMoverWithSetReplication(conf, allowChange);
  }

  public ZoneMoverWithSetReplication(NameNodeConnector nnc,
      Configuration conf, ReplicationRule rule,
      AtomicInteger retryCount, boolean allowChange) throws IOException {
    super(nnc, conf, rule, retryCount);
    initZoneMoverWithSetReplication(conf, allowChange);
  }

  public ZoneMoverWithSetReplication(NameNodeConnector nnc,
      Configuration conf, ReplicationRule rule,
      AtomicInteger retryCount, boolean allowChange, boolean fromZS) throws IOException {
    super(nnc, conf, rule, retryCount);
    initZoneMoverWithSetReplication(conf, allowChange);
    this.fromZS = fromZS;
  }

  public ZoneMoverWithSetReplication(NameNodeConnector nnc,
      Configuration conf, Map<String, ReplicationRule> pathRuleMap,
      AtomicInteger retryCount, boolean allowChange, boolean fromZS) throws IOException {
    super(nnc, conf, pathRuleMap, retryCount);
    initZoneMoverWithSetReplication(conf, allowChange);
    this.fromZS = fromZS;
  }

  void initZoneMoverWithSetReplication(Configuration conf, boolean allowChange) throws IOException {
    allowChangeReplication = allowChange;
    migrationRuleMap = new ReplicaMigrationRuleMap(conf);
    preMigrationCheckInterval = conf.getLong(
        DFSConfigKeys.DFS_ZONE_MIGRATION_PRE_MIGRATION_CHECK_INTERVAL_KEY,
        DFSConfigKeys.DFS_ZONE_MIGRATION_PRE_MIGRATION_CHECK_INTERVAL_DEFAULT);
    int preMigrationQueueSize = conf.getInt(
        DFSConfigKeys.DFS_ZONE_MIGRATION_PRE_MIGRATION_QUEUE_SIZE_KEY,
        DFSConfigKeys.DFS_ZONE_MIGRATION_PRE_MIGRATION_QUEUE_SIZE_DEFAULT);
    preMigrationFileQueue =
        new LinkedBlockingQueue<>(preMigrationQueueSize);
    preMigrationLatch = new CountDownLatch(2);
    preMigrationChecker.start();
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

  @Override
  protected Processor initProcessor() {
    return new ProcessorWithSetReplication();
  }

  @Override
  protected Fetcher initFetcher(Processor processor) {
    return new FetcherWithPreMigration(processor);
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
          LOG.debug("Check path: " + curPath);
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
  void shutdown() {
    dispatcher.shutdownNow();
    preMigrationChecker.interrupt();
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
  
  class ProcessorWithSetReplication extends Processor {
    @Override
    protected void processPath(String fullPath, ReplicationRule rule, Mover.Result result,
        MigrationDataCenters dc) {
      LOG.info("Processing path: {}, mode: {}", fullPath, runMode);
      processPath(fullPath, rule, result, dc, true);
    }

    // stop coordinator after pre-process queue is empty
    @Override
    protected void stopCoordinator() {
      preMigrationLatch.countDown();
      try {
        preMigrationLatch.await();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      coordinator.waitForCheckCompletion();
    }

    @Override
    protected void processRecursively(String parent, HdfsFileStatus status, ReplicationRule rule,
        Mover.Result result, MigrationDataCenters dc) {
      String fullPath = status.getFullName(parent);
      if (status.isDir()) {
        if (!fullPath.endsWith(Path.SEPARATOR)) {
          fullPath = fullPath + Path.SEPARATOR;
        }
        processPath(fullPath, rule, result, dc);
      } else if (!status.isSymlink()) { // file
        preMigrationFile(fullPath, (HdfsLocatedFileStatus) status, rule, result, dc);
      }
    }

    /**
     * Generate the file rule
     * If the file need to be pre-migrated,
     * will pre-migrate it and put into preMigrationFileQueue}
     * */
    private void preMigrationFile(String fullPath,
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

      ReplicationRule appliedRule = rule;
      // Queue file here to avoid multiple incr file
      ZoneProgressTracker.queueFile(fullPath);

      if (allowChangeReplication) {
        LocatedBlock firstBlock = status.getBlockLocations().get(0);
        Map<String, Short> blockDistribution = getBlockDistribution(firstBlock);
        if (blockDistribution.size() == 0) {
          LOG.error("There are no replicas for the missing block {}", firstBlock);
          ZoneProgressTracker.dequeueFile(fullPath);
          return;
        }
        ReplicationRule dis = ReplicationRule.parseFromMap(blockDistribution);
        if (runMode == RunMode.BATCH) {
          appliedRule = migrationRuleMap.getRuleFromDistribution(dis, status.getReplication());
        } else if (runMode == RunMode.CHECK) {
          if (dc == null) {
            LOG.warn("Using check mode but not give the data center!");
            ZoneProgressTracker.dequeueFile(fullPath);
            return;
          }
          appliedRule = migrationRuleMap.checkDistribution(dis, status.getReplication(), dc);
          if (appliedRule == null) {
            ZoneProgressTracker.dequeueFile(fullPath);
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

        // If two replicas are required to migrate, the tool will migrate one replica first
        // then the rest replica can copy from the migrated replica directly
        if (appliedRule.getReplica(MigrationDataCenters.STT.getName()) > 1
            && !dis.getDatacenters().contains(MigrationDataCenters.STT.getName())
            && dis.getReplica() == status.getReplication()) {
          try {
            LOG.info("Will pre migration 1 replica from TL to STT for {}", fullPath);
            Map<String, Short> disMap = dis.toMap();
            disMap.put(MigrationDataCenters.TL.getName(),
                (short) (disMap.get(MigrationDataCenters.TL.getName()) - 1));
            disMap.put(MigrationDataCenters.STT.getName(), (short) 1);
            ReplicationRule preRule = ReplicationRule.parseFromMap(disMap);
            processFileBlocks(fullPath, status, preRule, result, true);
            preMigrationFileQueue.put(new PreMigrationFile(fullPath, appliedRule));
          } catch (InterruptedException e) {
            processFile(fullPath, status, appliedRule, result);
            LOG.warn("Add pre-migration file {} into pre-migration queue is interrupted", fullPath);
          }
        } else {
          processFile(fullPath, status, appliedRule, result);
        }
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
        }
      }
    }

    @Override
    protected void processFile(String fullPath,
        HdfsLocatedFileStatus status, ReplicationRule appliedRule,
        Mover.Result result) {
      final LocatedBlocks locatedBlocks = status.getBlockLocations();

      if (allowChangeReplication) {
        try {
          if (appliedRule.getReplica() != status.getReplication()) {
            long startRpcTime = Time.monotonicNow();
            LOG.debug("Before set replication: distribution is {}, appliedRule is {}",
                getBlockDistribution(status.getBlockLocations().get(0)), appliedRule);
            dfs.setReplication(fullPath, appliedRule.getReplica());
            ZoneProgressTracker.addSetReplicationTime(Time.monotonicNow() - startRpcTime);
          }
        } catch (IOException e) {
          LOG.warn("Set replication fails for {}\n {}", fullPath, e);
          result.setRetryFailed();
        }
      } else {
        LOG.warn("Ignore replica not consistent file: {}", fullPath);
        ZoneProgressTracker.dequeueFile(fullPath);
        return;
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
        ZoneProgressTracker.dequeueFile(fullPath);
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

    @Override
    protected Dispatcher.DBlock newDBlock(LocatedBlock lb, List<Mover.MLocation> locations,
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
  }

  class FetcherWithPreMigration extends Fetcher {
    FetcherWithPreMigration(Processor processor) {
      super(processor, "ZoneMover-Fetcher-PreMigration");
    }

    @Override
    public void run() {
      LOG.info("Fetcher with pre-migration is started!");
      long lastRecord = Time.monotonicNow();
      ZoneReplicationCoordinator.FileState fileState = null;
      while (true) {
        try {
          fileState = coordinator.getNextFinishedFile();
          processor.processFileBlocks(fileState.getFilePath(),
              fileState.getFileStatus(), fileState.getRule(), result, true);
          lastRecord = Time.monotonicNow();
        } catch (NoSuchElementException e) {
          if ((Time.monotonicNow() - lastRecord) > 2 * preMigrationCheckInterval) {
            LOG.info("Fetcher for replication mismatch file is timeout, stopping...");
            break;
          }
        } catch (Exception e) {
          LOG.warn("Fetcher encountered the exception!", e);
        } finally {
          if (fileState != null) {
            try {
              ZoneProgressTracker.dequeueFile(fileState.getFilePath());
              fileState = null;
            } catch (NullPointerException e) {
              LOG.warn("Dequeue file {} fail with null exception", fileState.getFilePath());
            }
          }
        }
      }
    }
  }

  /**
   * A structure used to record the path-rule pair.
   */
  static class PreMigrationFile {
    private final String filePath;
    private final long recordTime;
    private final ReplicationRule rule;

    PreMigrationFile(String filePath, ReplicationRule rule) {
      this.filePath = filePath;
      this.rule = rule;
      recordTime = Time.monotonicNow();
    }

    public String getFilePath() {
      return filePath;
    }

    public long getRecordTime() {
      return recordTime;
    }

    public ReplicationRule getRule() {
      return rule;
    }
  }

  /**
   * Check if the pre-migration file has waited for enough time to proceed to the next step
   * */
  class PreMigrationChecker implements Runnable {
    @Override
    public void run() {
      LOG.info("Pre-process checker is started.");
      PreMigrationFile preMigrationFile;
      while (true) {
        try {
          preMigrationFile = preMigrationFileQueue.poll();
          if (preMigrationFile == null) {
            continue;
          }
          if ((Time.monotonicNow() - preMigrationFile.getRecordTime()) >
              preMigrationCheckInterval) {
            HdfsLocatedFileStatus status = (HdfsLocatedFileStatus) dfs.listPaths(
                preMigrationFile.getFilePath(), HdfsFileStatus.EMPTY_NAME, true)
                .getPartialListing()[0];
            processor.processFile(preMigrationFile.getFilePath(), status,
                preMigrationFile.getRule(), result);
          } else {
            preMigrationFileQueue.put(preMigrationFile);
          }
          if (preMigrationLatch.getCount() == 1 && preMigrationFileQueue.isEmpty()) {
            preMigrationLatch.countDown();
            return;
          }
        } catch (Exception e) {
          LOG.warn("Pre-migration checker encountered the exception!", e);
        }
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
        pathRuleMap = pathRuleMapTmp;
        zoneMoverTrigger.updatePaths(ZoneMover.Cli.getPaths(pathRuleMap));
      } else {
        // Update the filteredPathRulesFromZS for Zone Mover.
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
