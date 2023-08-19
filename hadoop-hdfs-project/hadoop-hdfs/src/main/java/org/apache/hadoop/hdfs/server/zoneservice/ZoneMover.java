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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.net.DFSNetworkTopologyWithDataCenter;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher.*;
import org.apache.hadoop.hdfs.server.mover.Mover;
import org.apache.hadoop.hdfs.server.mover.Mover.MLocation;
import org.apache.hadoop.hdfs.server.mover.Mover.Result;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher.DDatanode.StorageGroup;
import org.apache.hadoop.hdfs.server.balancer.ExitStatus;
import org.apache.hadoop.hdfs.server.balancer.NameNodeConnector;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyWithDataCenter;
import org.apache.hadoop.hdfs.server.namenode.UnsupportedActionException;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.zoneservice.ZoneDispatcher.ZoneSource;
import org.apache.hadoop.hdfs.server.zoneservice.ZoneDispatcher.ZoneDDatanode;
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
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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
public class ZoneMover {
  private static final Logger LOG = LoggerFactory.getLogger(ZoneMover.class);
  protected static final String ROOT = "/";
  private static final String ZONEMOVER_ID_PATH = "/system/zonemover.id";
  protected final ZoneDispatcher dispatcher;
  protected final StorageMap storages;
  protected final List<Path> targetPaths;
  protected final int retryMaxAttempts;
  protected ReplicationRule globalRule = null;
  protected Map<String, ReplicationRule> pathRuleMap = Maps.newHashMap();
  // Filtered path rules from ZoneService for ZoneMover.
  protected Map<String, ReplicationRule> filteredPathRulesFromZS = Maps.newHashMap();
  protected final AtomicInteger retryCount;
  protected final DFSClient dfs;
  protected static final long DELAY_AFTER_CHOOSE_FAIL = 2 * 1000;
  protected final Processor processor;
  public final ReplicationRuleUtil ruleUtil;
  protected final boolean xattrSetEnable;
  protected final ZoneReplicationCoordinator coordinator;
  protected Result result;
  protected final Thread fetcher;
  protected static int checkUpdateInterval = 0;
  protected static final String DC_SEPARATOR = ",";

  public ZoneMover(NameNodeConnector nnc,
      Configuration conf, AtomicInteger retryCount) {
    final long movedWinWidth = conf.getLong(
        DFSConfigKeys.DFS_ZONEMOVER_MOVEDWINWIDTH_KEY,
        DFSConfigKeys.DFS_ZONEMOVER_MOVEDWINWIDTH_DEFAULT);
    final int dispatcherThreads = conf.getInt(
        DFSConfigKeys.DFS_ZONEMOVER_DISPATCHERTHREADS_KEY,
        DFSConfigKeys.DFS_ZONEMOVER_DISPATCHERTHREADS_DEFAULT);
    final int maxConcurrentMovesPerNode = conf.getInt(
        DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY,
        DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_DEFAULT);
    this.retryMaxAttempts = conf.getInt(
        DFSConfigKeys.DFS_ZONEMOVER_RETRY_MAX_ATTEMPTS_KEY,
        DFSConfigKeys.DFS_ZONEMOVER_RETRY_MAX_ATTEMPTS_DEFAULT);
    this.retryCount = retryCount;
    final int blockDispatchAttempts = conf.getInt(
        DFSConfigKeys.DFS_ZONEMOVER_BLOCK_DISPATCH_ATTEMPTS_KEY,
        DFSConfigKeys.DFS_ZONEMOVER_BLOCK_DISPATCH_ATTEMPTS_DEFAULT);
    final long blockDispatchRetryInterval = conf.getLong(
        DFSConfigKeys.DFS_ZONEMOVER_BLOCK_DISPATCH_RETRY_INTERVAL_KEY,
        DFSConfigKeys.DFS_ZONEMOVER_BLOCK_DISPATCH_RETRY_INTERVAL_DEFAULT);
    final long dispatcherKeepAliveTime = conf.getLong(
        DFSConfigKeys.DFS_ZONEMOVER_DISPATCHER_KEEP_ALIVE_TIME_KEY,
        DFSConfigKeys.DFS_ZONEMOVER_DISPATCHER_KEEP_ALIVE_TIME_DEFAULT);
    final long dispatcherThrottlerBandwidth = conf.getLong(
        DFSConfigKeys.DFS_ZONEMOVER_DISPATCHER_THROTTLER_BANDWIDTH_KEY,
        DFSConfigKeys.DFS_ZONEMOVER_DISPATCHER_THROTTLER_BANDWIDTH_DEFAULT);
    this.dispatcher = new ZoneDispatcher(nnc, Collections.emptySet(),
        Collections.emptySet(), movedWinWidth, dispatcherThreads,
        maxConcurrentMovesPerNode, conf,
        blockDispatchAttempts, blockDispatchRetryInterval, dispatcherKeepAliveTime,
        dispatcherThrottlerBandwidth);
    this.dfs = this.dispatcher.getDistributedFileSystem().getClient();
    this.storages = new StorageMap();
    this.targetPaths = nnc.getTargetPaths();
    this.ruleUtil = new ReplicationRuleUtil(this.dispatcher.getDistributedFileSystem());
    this.xattrSetEnable = conf.getBoolean(
        DFSConfigKeys.DFS_ZONEMOVER_XATTR_SET_ENABLE_KEY,
        DFSConfigKeys.DFS_ZONEMOVER_XATTR_SET_ENABLE_DEFAULT);
    this.coordinator = new ZoneReplicationCoordinator(
        conf, this.dispatcher.getDistributedFileSystem());
    checkUpdateInterval = conf.getInt(
        DFSConfigKeys.DFS_ZONEMOVER_CHECK_ZK_UPDATE_PATH_RULE_MAP_INTERVAL,
        DFSConfigKeys.DFS_ZONEMOVER_CHECK_ZK_UPDATE_PATH_RULE_MAP_INTERVAL_DEFAULT);
    processor = initProcessor();
    fetcher = new Fetcher(processor);
    fetcher.start();
  }

  protected Processor initProcessor() {
    return new Processor();
  }

  public ZoneMover(NameNodeConnector nnc, Configuration conf,
      ReplicationRule rule, AtomicInteger retryCount) {
    this(nnc, conf, retryCount);
    this.globalRule = rule;
  }

  public ZoneMover(NameNodeConnector nnc, Configuration conf,
      Map<String, ReplicationRule> pathRuleMap, AtomicInteger retryCount) {
    this(nnc, conf, retryCount);
    this.pathRuleMap = pathRuleMap;
  }

  /**
   * Check if zonemover is compatible with the block placement policy
   * used by the NameNode.
   */
  protected static void checkReplicationPolicyCompatibility(Configuration conf)
      throws UnsupportedActionException {
    String clazz = conf.get(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY);
    if (clazz == null || !clazz.equals(
        BlockPlacementPolicyWithDataCenter.class.getName())) {
      throw new UnsupportedActionException(
          "ZoneMover must work with BlockPlacementPolicyWithDataCenter");
    }
  }

  void init(Configuration conf) throws IOException {
    LOG.info("Initializing ...");
    final List<DatanodeStorageReport> reports = dispatcher.init();
    ZoneProgressTracker.initConf(conf);
    LOG.info("Datanode reports size: " + reports.size());
    for (DatanodeStorageReport r: reports) {
      final ZoneDDatanode dn = dispatcher.newZoneDDatanode(r.getDatanodeInfo());
      for (StorageType t: StorageType.getMovableTypes()) {
        final ZoneSource source = dn.addSource(t, Long.MAX_VALUE, dispatcher);
        final long maxRemaining = Mover.getMaxRemaining(r, t);
        final StorageGroup target = maxRemaining > 0L ? dn.addTarget(t, maxRemaining) : null;
        storages.add(source, target);
      }
    }
  }

  /* run the process logic */
  ExitStatus run() {
    try {
      return new Processor().processPath().getExitStatus();
    } catch (IllegalArgumentException e) {
      System.out.println(e + ".  Exiting ...");
      return ExitStatus.ILLEGAL_ARGUMENTS;
    }
  }

  /* Note that this method does not call waitForCheckCompletion.
   * It is generally running in monitor mode.
   */
  ExitStatus run(String path) throws IllegalArgumentException {
    Result result = new Result();
    this.result = result;
    if (this.globalRule != null) {
      processor.processPath(path, this.globalRule, result, null);
    } else {
      processor.processPath(path, getPathRule(path), result, null);
    }

    return result.getExitStatus();
  }


  ExitStatus run(String path, long fileId) throws IllegalArgumentException {
    Result result = new Result();
    this.result = result;
    try {
      processor.processFileById(path, fileId, result);
    } catch (Throwable e) {
      LOG.error("Process file by id fail", e);
      return ExitStatus.IO_EXCEPTION;
    }
    return result.getExitStatus();
  }

  /**
   * Get the corresponding rule for the path.
   * @param path to apply a rule
   * @return the corresponding rule
   */
  ReplicationRule getPathRule(String path) throws IllegalArgumentException {
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
    if (matchPath.length() > 0) {
      return pathRuleMap.get(matchPath);
    } else {
      throw new IllegalArgumentException("No rule for path: " + path);
    }
  }

  /**
   * Filters path rules from "ZoneService" for ZoneMover.
   * @param path
   * @return
   */
  ReplicationRule getFilteredPathRulesFromZS(String path) {
    String matchPath = "";
    for (Map.Entry<String, ReplicationRule> entry: filteredPathRulesFromZS.entrySet()) {
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
    if (matchPath.length() > 0) {
      return filteredPathRulesFromZS.get(matchPath);
    } else {
      return null;
    }
  }

  /* release resources */
  void shutdown() {
    dispatcher.shutdownNow();
  }

  /* reset success and failure states of targets */
  void resetTargetsStatus() {
    for (StorageGroup node: storages.targets.values()) {
      node.getDDatanode().resetStatus();
    }
  }

  /**
   * Run with prepared arguments.
   * @param conf configuration
   * @param namenode URI of the NameNode
   * @param paths paths to apply the rule
   * @param rule the rule to apply
   * @return a ExitStatus code
   */
  public static int run(Configuration conf, URI namenode,
      List<Path> paths, ReplicationRule rule)
      throws IOException, InterruptedException {
    return ZoneMover.run(conf, namenode, paths, rule, null);
  }

  /**
   * Run with prepared arguments.
   * @param conf configuration
   * @param namenode URI of the NameNode
   * @param paths paths to apply the rule
   * @param pathRuleMap map of paths and rules
   * @return a ExitStatus code
   */
  public static int run(Configuration conf, URI namenode,
      List<Path> paths, Map<String, ReplicationRule> pathRuleMap)
      throws IOException, InterruptedException {
    return ZoneMover.run(conf, namenode, paths, null, pathRuleMap);
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
  public static int run(Configuration conf, URI namenode,
      List<Path> paths, ReplicationRule rule,
      Map<String, ReplicationRule> pathRuleMap)
      throws IOException, InterruptedException {
    ZoneProgressTracker.startCountingInitTime();
    checkDataCenterValues(conf, rule, pathRuleMap);
    LOG.info("Start to apply rule: " + rule + " to namenode:"
        + namenode + ", path: " + paths);
    if (paths.isEmpty()) {
      ZoneProgressTracker.finishCountingInitTimeAndLog();
      return ExitStatus.SUCCESS.getExitCode();
    }

    NameNodeConnector nnc = null;
    ZoneMover zs = null;
    // retryCount starts from 0 and ends at retryMaxAttempts
    AtomicInteger retryCount = new AtomicInteger(0);
    final long sleepTime = calculateSleepTime(conf);
    final boolean exitEvenHasProgress = conf.getBoolean(
        DFSConfigKeys.DFS_ZONEMOVER_EXIT_EVEN_HAS_PROGRESS,
        DFSConfigKeys.DFS_ZONEMOVER_EXIT_EVEN_HAS_PROGRESS_DEFAULT);

    try {
      // Set maxNotChangedIterations to 1 as ZoneMover does not need to loop
      nnc = new NameNodeConnector(ZoneMover.class.getSimpleName(),
          namenode, getIdPath(RunMode.BATCH), paths, conf, 1);
      nnc.getKeyManager().startBlockKeyUpdater();
      if (rule != null) {
        zs = new ZoneMover(nnc, conf, rule, retryCount);
      } else {
        zs = new ZoneMover(nnc, conf, pathRuleMap, retryCount);
      }
      zs.init(conf);
      int round = 0;

      ZoneProgressTracker.finishCountingInitTimeAndLog();

      while (true) {
        round += 1;
        LOG.info("Start round " + round + " ...");
        final ExitStatus r= zs.run();
        if (r == ExitStatus.SUCCESS) {
          break;
        } else if (r != ExitStatus.IN_PROGRESS) {
          if (r == ExitStatus.NO_MOVE_PROGRESS) {
            System.err.println("Failed to move some blocks after "
                + zs.retryMaxAttempts + " retries. Exiting...");
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
        zs.resetTargetsStatus();
        //noinspection BusyWait
        Thread.sleep(sleepTime);
      }
    } finally {
      if (nnc != null) {
        IOUtils.cleanupWithLogger(LOG, nnc);
      }
      ZoneProgressTracker.checkForLeak();
      if (zs != null) {
        zs.shutdown();
      }
    }

    return ExitStatus.SUCCESS.getExitCode();
  }

  /**
   * Run with prepared arguments for monitorByTrigger mode.
   * @param conf configuration
   * @param namenode URI of the NameNode
   * @return a ExitStatus code
   */
  public static int run(Configuration conf, URI namenode, boolean loadMapFromStore)
      throws IOException {
    List<Path> paths = new ArrayList<>();
    return run(new ZoneMoverKafkaTrigger(conf, paths, namenode),
        conf, namenode, paths, null, new HashMap<>(), loadMapFromStore);
  }

  /**
   * Run with prepared arguments for monitorByTrigger mode.
   * @param zoneMoverTrigger trigger for ZoneMover monitor
   * @param conf configuration
   * @param namenode URI of the NameNode
   * @param paths paths to apply the rule
   * @param rule the rule to apply
   * @return a ExitStatus code
   */
  public static int run(ZoneMoverTrigger zoneMoverTrigger,
      Configuration conf, URI namenode, List<Path> paths,
      ReplicationRule rule, boolean loadMapFromStore) throws IOException {
    return run(zoneMoverTrigger, conf, namenode, paths, rule, null, loadMapFromStore);
  }

  /**
   * Run with prepared arguments for monitorByTrigger mode.
   * @param zoneMoverTrigger trigger for ZoneMover monitor
   * @param conf configuration
   * @param namenode URI of the NameNode
   * @param paths paths to apply the rule
   * @param pathRuleMap map of paths and rules
   * @return a ExitStatus code
   */
  public static int run(ZoneMoverTrigger zoneMoverTrigger, Configuration conf,
      URI namenode, List<Path> paths, Map<String, ReplicationRule> pathRuleMap,
      boolean loadMapFromStore) throws IOException {
    return run(zoneMoverTrigger, conf, namenode, paths, null, pathRuleMap, loadMapFromStore);
  }

  /**
   * Run with prepared arguments for monitorByTrigger mode.
   * @param zoneMoverTrigger trigger for ZoneMover monitor
   * @param conf configuration
   * @param namenode URI of the NameNode
   * @param paths paths to apply the rule
   * @param rule the rule to apply
   * @param pathRuleMap map of paths and rules
   * @return a ExitStatus code
   */
  private static int run(ZoneMoverTrigger zoneMoverTrigger,
      Configuration conf, URI namenode, List<Path> paths,
      ReplicationRule rule, Map<String, ReplicationRule> pathRuleMap,
      boolean loadMapFromStore) throws IOException {
    if (!loadMapFromStore) {
      try {
        if (ExitStatus.SUCCESS.getExitCode() != run(conf, namenode, paths, rule, pathRuleMap)) {
          LOG.error("Move the original data for {} in {} fail.", paths, namenode.getAuthority());
        }
      } catch (InterruptedException e) {
        LOG.error("Batch process is interrupted.", e);
      }
    }
    checkDataCenterValues(conf, rule, pathRuleMap);
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
    NameNodeConnector nnc = null;
    ZoneMover zs = null;
    Thread mapUpdaterThread;
    StoreDriver driver;
    String ns = namenode.getAuthority();
    LOG.info("Initializing NameNodeConnector");
    try {
      nnc = new NameNodeConnector(ZoneMover.class.getSimpleName(),
          namenode, getIdPath(RunMode.MONITOR), paths, conf, 1);
      nnc.getKeyManager().startBlockKeyUpdater();
      if (rule != null) {
        zs = new ZoneMover(nnc, conf, rule, new AtomicInteger(0));
      } else {
        zs = new ZoneMover(nnc, conf, pathRuleMap, new AtomicInteger(0));
      }
      zs.init(conf);
      // Monitor if the path rule map is update or not when zk enable
      if (loadMapFromStore) {
        LOG.info("Initializing MapUpdater");
        driver = ReflectionUtils.newInstance(driverClass, conf);
        driver.init(conf, "ZoneMover_" + namenode.getAuthority());
        MapUpdater mapUpdater =
            zs.new MapUpdater(namenode, driver, zoneMoverTrigger);
        mapUpdaterThread = new Thread(mapUpdater, "Updater" + namenode.getAuthority());
        mapUpdaterThread.start();
      }
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

  static Path getIdPath(RunMode mode) {
    return new Path(String.format("%s.%s.%s.%s",
        ZONEMOVER_ID_PATH,
        mode.toString().toLowerCase(),
        NetUtils.getLocalHostname(),
        Time.now()));
  }

  protected static long calculateSleepTime(final Configuration conf) {
    return conf.getTimeDuration(
            DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
            DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT,
            TimeUnit.SECONDS, TimeUnit.MILLISECONDS) +
        conf.getTimeDuration(
            DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY,
            DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_DEFAULT,
            TimeUnit.SECONDS, TimeUnit.MILLISECONDS) * 3;
  }

  /**
   * Get the datacenter distribution of the block.
   */
  static Map<String, Short> getBlockDistribution(LocatedBlock block) {
    Map<String, Short> distribution = new HashMap<>();
    // Count replica in each datacenter
    DatanodeInfo[] infos = block.getLocations();
    for (DatanodeInfo info: infos) {
      String dc = DFSNetworkTopologyWithDataCenter.getDataCenter(
          info.getNetworkLocation());
      if (distribution.containsKey(dc)) {
        distribution.put(dc, (short) (distribution.get(dc) + 1));
      } else {
        distribution.put(dc, (short) 1);
      }
    }
    return distribution;
  }
  /**
   * Check if a block already satisfies the rule.
   */
  boolean isBlockSatisfyRule(LocatedBlock block, ReplicationRule rule) {
    return rule.equals(
        ReplicationRule.parseFromMap(getBlockDistribution(block)));
  }

  /**
   * Check if all blocks in the list have the same datacenter distribution.
   */
  static boolean areBlocksDistributionConsistent(
      List<LocatedBlock> blocks) {
    boolean consistent = true;
    Map<String, Short> distribution = null;
    for (LocatedBlock block: blocks) {
      if (distribution == null) {
        distribution = getBlockDistribution(block);
      } else {
        if (!distribution.equals(getBlockDistribution(block))) {
          consistent = false;
          break;
        }
      }
    }
    return consistent;
  }
  /**
   * Get {@link ZoneMoveItem} list for the block.
   */
  List<ZoneMoveItem> getZoneMoveItems(final LocatedBlock block, ReplicationRule rule) {
    // calculate sources and targets
    Map<String, Short> distribution = getBlockDistribution(block);
    Map<String, Short> sources = new HashMap<>();
    Map<String, Short> targets = new HashMap<>();
    Set<String> dcs = new HashSet<>();
    Map<String, Short> ruleMap = rule.toMap();
    dcs.addAll(distribution.keySet());
    dcs.addAll(ruleMap.keySet());
    for (String dc: dcs) {
      short m = 0;
      if (distribution.containsKey(dc)) {
        m = distribution.get(dc);
      }
      short n = 0;
      if (ruleMap.containsKey(dc)) {
        n = ruleMap.get(dc);
      }
      if (m > n) {
        sources.put(dc, (short) (m - n));
      } else if (m < n) {
        targets.put(dc, (short) (n - m));
      }
    }

    // generate ZoneMoveItem
    List<ZoneMoveItem> items = new ArrayList<>();
    for (Map.Entry<String, Short> sourceEntry: sources.entrySet()) {
      String sdc = sourceEntry.getKey();
      short x = sourceEntry.getValue();
      for (String tdc: new HashSet<>(targets.keySet())) {
        short y = targets.get(tdc);
        if (x == y) {
          items.add(new ZoneMoveItem(sdc, tdc, x));
          targets.remove(tdc);
          break;
        } else if (x > y) {
          items.add(new ZoneMoveItem(sdc, tdc, y));
          targets.remove(tdc);
          x = (short) (x - y);
        } else {
          items.add(new ZoneMoveItem(sdc, tdc, x));
          targets.put(tdc, (short) (y - x));
          break;
        }
      }
    }
    return items;
  }

  static void checkDataCenterValues(
      Configuration conf, Collection<ReplicationRule> rules)
      throws IllegalArgumentException {
    // get valid datacenters from configuration
    String datacenters = conf.get(
        DFSConfigKeys.DFS_ZONEMOVER_VALID_DATACENTERS_KEY,
        DFSConfigKeys.DFS_ZONEMOVER_VALID_DATACENTERS_DEFAULT);
    Set<String> validDataCenters = new HashSet<>
        (Arrays.asList(datacenters.trim().split(DC_SEPARATOR)));
    validDataCenters.remove("");

    // check datacenters in rules
    for (ReplicationRule rule: rules) {
      for (ReplicationRuleSection section: rule.getSections()) {
        if (!validDataCenters.contains(section.getDataCenter())) {
          throw new IllegalArgumentException(
              section.getDataCenter() + " is NOT a valid DataCenter!");
        }
      }
    }
  }

  static void checkDataCenterValues(Configuration conf,
      ReplicationRule rule, Map<String, ReplicationRule> pathRuleMap)
      throws IllegalArgumentException {
    Set<ReplicationRule> rules = new HashSet<>();
    if (rule != null) {
      rules.add(rule);
    }
    if (pathRuleMap != null) {
      rules.addAll(pathRuleMap.values());
    }
    checkDataCenterValues(conf, rules);
  }

  static class ZoneMoveItem {
    protected final String sourceDataCenter;
    protected final String targetDataCenter;
    protected final short num;

    /**
     * @param sourceDataCenter source data center
     * @param targetDataCenter target data center
     * @param num replicas to move
     */
    ZoneMoveItem(@Nonnull final String sourceDataCenter,
        @Nonnull final String targetDataCenter, final short num) {
      this.sourceDataCenter = sourceDataCenter;
      this.targetDataCenter = targetDataCenter;
      this.num = num;
    }

    public String getSourceDataCenter() {
      return sourceDataCenter;
    }

    public String getTargetDataCenter() {
      return targetDataCenter;
    }

    public short getNum() {
      return num;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ZoneMoveItem that = (ZoneMoveItem) o;
      return num == that.num && sourceDataCenter.equals(
          that.sourceDataCenter) && targetDataCenter.equals(that.targetDataCenter);
    }

    @Override
    public int hashCode() {
      return Objects.hash(sourceDataCenter, targetDataCenter, num);
    }

    @Override
    public String toString() {
      return "ZoneMoveItem{" +
          "source='" + sourceDataCenter + '\'' +
          ", target='" + targetDataCenter + '\'' +
          ", num=" + num +
          '}';
    }
  }

  /* Keep StorageGroupMap for sources and targets */
  protected static class StorageMap {
    public final StorageGroupMap<ZoneSource> sources = new StorageGroupMap<>();
    public final StorageGroupMap<StorageGroup> targets = new StorageGroupMap<>();
    public final HashMap<String, EnumMap<StorageType, List<StorageGroup>>> dcTargetStorageTypeMap =
        new HashMap<>();

    private StorageMap() {
    }

    private void add(ZoneSource source, StorageGroup target) {
      sources.put(source);
      if (target != null) {
        targets.put(target);
        String dc = DFSNetworkTopologyWithDataCenter.getDataCenter(
            source.getDatanodeInfo().getNetworkLocation());
        getTargetStorages(target.getStorageType(), dc).add(target);
      }
    }

    public ZoneSource getSource(MLocation ml) {
      return get(sources, ml);
    }

    public StorageGroup getTarget(MLocation ml) {
      return get(targets, ml);
    }

    private static <G extends StorageGroup> G get(StorageGroupMap<G> map, MLocation ml) {
      return map.get(ml.getDatanode().getDatanodeUuid(), ml.getStorageType());
    }

    protected List<StorageGroup> getTargetStorages(StorageType st, String datacenter) {
      if (!dcTargetStorageTypeMap.containsKey(datacenter)) {
        EnumMap<StorageType, List<StorageGroup>> em = new EnumMap<>(StorageType.class);
        for (StorageType t : StorageType.getMovableTypes()) {
          em.put(t, new LinkedList<>());
        }
        dcTargetStorageTypeMap.put(datacenter, em);
      }
      return dcTargetStorageTypeMap.get(datacenter).get(st);
    }
  }

  class Processor {

    protected Result processPath() {
      return processPath(null);
    }

    protected Result processPath(MigrationDataCenters dc) {
      ZoneProgressTracker.resetTracker();
      ZoneProgressTracker.trackPaths(dispatcher.getDistributedFileSystem(), targetPaths);
      ZoneProgressTracker.startCountingProcessPathTime();

      result = new Result();
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
      boolean hasFailed = dispatcher.waitForMoveCompletion(storages.targets.values());
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

    protected void processPath(String fullPath, ReplicationRule rule, Result result,
        MigrationDataCenters dc) {
      LOG.info("Processing path: " + fullPath + " ...");
      processPath(fullPath, rule, result, dc, false);
    }

    protected void processPath(String fullPath, ReplicationRule rule, Result result,
        MigrationDataCenters dc, boolean doesMsync) {
      for (byte[] lastReturnedName = HdfsFileStatus.EMPTY_NAME;;) {
        final DirectoryListing children;
        try {
          // Only call msync for the root path. Avoid stale read for trigger mode.
          if (doesMsync) {
            dfs.msync();
          }
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
        Result result, MigrationDataCenters dc) {
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

    private void processFileById(String fullPath, long fileId, Result result) throws IOException {
      HdfsFileStatus[] statuses = dfs.listPaths(fullPath, fileId,
          HdfsFileStatus.EMPTY_NAME, true).getPartialListing();

      if (statuses[0].isDir()) {
        LOG.info("Skip directory: " + fullPath);
        return;
      }
      Preconditions.checkArgument(statuses[0] instanceof HdfsLocatedFileStatus);
      HdfsLocatedFileStatus status = (HdfsLocatedFileStatus) statuses[0];
      final LocatedBlocks locatedBlocks = status.getLocatedBlocks();
      if (status.getLen() == 0) {
        LOG.info("Skip empty file: " + fullPath);
        return;
      }

      if (!locatedBlocks.isLastBlockComplete()) {
        LOG.info("Skip uncompleted file: " + fullPath);
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
      processFileBlocks(fullPath, status, rule, result);
    }

    protected void processFile(String fullPath, HdfsLocatedFileStatus status, ReplicationRule rule,
        Result result, MigrationDataCenters dc) {
      LOG.info("Processing file: " + fullPath + " ....");

      final LocatedBlocks locatedBlocks = status.getLocatedBlocks();
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

      if (xattrSetEnable) {
        boolean needSet = true;
        try {
          if (ruleUtil.hasRuleInXAttr(fullPath)) {
            try {
              if (ruleUtil.getRuleFromXAttr(fullPath).equals(rule)) {
                LOG.info("This file already has the replicationRule: " + fullPath);
                needSet = false;
              }
            } catch (IllegalArgumentException e) {
              LOG.warn("Found an invalid rule in NameNode, will update it: file={}, content={}",
                  fullPath, ruleUtil.getStringFromRuleKey(fullPath));
            }
          }

          if (needSet) {
            ruleUtil.setRuleToXAttr(fullPath, rule);
            LOG.info("Added replicationRule to: " + fullPath);
          }
        } catch (IOException e) {
          LOG.warn(e.toString());
          ZoneProgressTracker.incrFileCount();
          return;
        }
      } else if (status.getReplication() != rule.getReplica()) {
        LOG.warn("Ignore replica not consistent file: {}", fullPath);
        ZoneProgressTracker.incrFileCount();
        return;
      }

      if (status.getReplication() < rule.getReplica()) {
        LOG.debug("factor < rule.replication file: " + fullPath);
        // For cases like changing "/Telin-3:3" to "/Telin-3:3,/Telin-4:2",
        // ZoneMover does not need to wait for NameNode to schedule replication.
        // For cases like changing "/Telin-3:3" to "/Telin-3:2,/Telin-4:3",
        // ZoneMover needs to wait for NameNode to schedule first and then
        // do redistribution. If ZoneMover does not wait, it will increase
        // the number of inter-dc block replications.
        if (isFileNeedMove(locatedBlocks, rule)) {
          coordinator.addFile(fullPath, rule,
              rule.getReplica() - status.getReplication(),
              locatedBlocks.getLocatedBlocks().size());
        }
      } else if (status.getReplication() > rule.getReplica()) {
        LOG.debug("factor > rule.replication file: " + fullPath);
        // For cases like changing "/Telin-3:3,/Telin-4:2" -> "/Telin-3:3",
        // ZoneMover does not need to wait.
        // For cases like changing "/Telin-3:3" to "/Telin-3:1,/Telin-4:1",
        // ZoneMover needs to wait. Otherwise, NameNode may schedule the
        // datanode to delete the replica first, which is also used as the
        // proxy of replaceBlock scheduled by ZoneMover.
        if (isFileNeedMove(locatedBlocks, rule)) {
          coordinator.addFile(fullPath, rule,
              rule.getReplica() - status.getReplication(),
              locatedBlocks.getLocatedBlocks().size());
        }
      } else {
        processFileBlocks(fullPath, status, rule, result);
      }
    }

    protected boolean isFileNeedMove(LocatedBlocks locatedBlocks, ReplicationRule rule) {
      for (LocatedBlock block : locatedBlocks.getLocatedBlocks()) {
        if (!getZoneMoveItems(block, rule).isEmpty()) {
          return true;
        }
      }
      return false;
    }

    protected void processFileBlocks(String fullPath, HdfsLocatedFileStatus status,
        ReplicationRule rule, Result result) {
      final LocatedBlocks locatedBlocks = status.getLocatedBlocks();
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
        LocatedBlocks locatedBlocks, ReplicationRule rule, Result result) {
      LocatedBlock firstBlock = locatedBlocks.get(0);
      if (isBlockSatisfyRule(firstBlock, rule)) {
        ZoneProgressTracker.incrFileCount();
        return;
      }
      List<ZoneMoveItem> moveItems = getZoneMoveItems(firstBlock, rule);

      // Do a dummy queue here to ensure the last dequeue of this path
      // is either the last dispatch executed or the end of this method, whichever happens later
      ZoneProgressTracker.queueFile(fullPath);

      int n = locatedBlocks.locatedBlockCount();
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
        LocatedBlocks locatedBlocks, ReplicationRule rule, Result result) {
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

    protected boolean scheduleMoves4Block(String fullPath, LocatedBlock lb,
        List<ZoneMoveItem> moveItems) {
      return scheduleMoves4Block(fullPath, lb, moveItems, null);
    }

    boolean scheduleMoves4Block(String fullPath, LocatedBlock lb, List<ZoneMoveItem> moveItems,
        ErasureCodingPolicy ecPolicy) {
      final List<MLocation> locations = MLocation.toLocations(lb);
      // put locations to a map with datacenter as the key
      final Map<String, List<MLocation>> locationMap = new HashMap<>();
      for (MLocation ml : locations) {
        String dc = DFSNetworkTopologyWithDataCenter.getDataCenter(ml.getDatanode().getNetworkLocation());
        if (locationMap.containsKey(dc)) {
          locationMap.get(dc).add(ml);
        } else {
          locationMap.put(dc, new ArrayList<>(Collections.singletonList(ml)));
        }
      }

      final DBlock db = newDBlock(lb, locations, ecPolicy);
      Set<StorageType> targetTypes = new HashSet<>(Arrays.asList(lb.getStorageTypes()));
      Set<StorageGroup> excluded = getExcluded(locations, moveItems);
      // get MLocation according to datacenter and select source
      for (ZoneMoveItem moveItem : moveItems) {
        for (short i = 0; i < moveItem.getNum(); i++) {
          List<MLocation> sourceLocations = locationMap.get(moveItem.getSourceDataCenter());
          MLocation location = sourceLocations.get(0);
          sourceLocations.remove(0);
          ZoneSource source = storages.getSource(location);
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

    protected DBlock newDBlock(LocatedBlock lb, List<MLocation> locations, ErasureCodingPolicy ecPolicy) {
      Block block = lb.getBlock().getLocalBlock();
      final DBlock db = new DBlock(block);
      for(MLocation ml : locations) {
        StorageGroup source = storages.getSource(ml);
        if (source != null) {
          db.addLocation(source);
        }
      }
      return db;
    }

    private Set<StorageGroup> getExcluded(
        List<MLocation> locations, List<ZoneMoveItem> moveItems) {
      Set<String> targetDataCenters = new HashSet<>();
      for (ZoneMoveItem item: moveItems) {
        targetDataCenters.add(item.targetDataCenter);
      }

      Set<StorageGroup> excluded = new HashSet<>();
      for (MLocation ml: locations) {
        String dc = DFSNetworkTopologyWithDataCenter.getDataCenter(
            ml.getDatanode().getNetworkLocation());
        if (targetDataCenters.contains(dc)) {
          excluded.add(storages.getTarget(ml));
        }
      }
      return excluded;
    }

    boolean scheduleMoveReplica(String fullPath, DBlock db, ZoneSource source, String targetDataCenter,
        Set<StorageType> targetTypes, Set<StorageGroup> excluded) {
      return chooseTargetInDataCenter(fullPath,
          db, source, targetDataCenter, targetTypes, excluded);
    }

    /**
     * Choose a storage in the datacenter.
     */
    boolean chooseTargetInDataCenter(String fullPath,
        DBlock db, ZoneSource source, String targetDataCenter,
        Set<StorageType> targetTypes, Set<StorageGroup> excluded) {
      for (StorageType t: targetTypes) {
        final List<StorageGroup> targets = storages.getTargetStorages(t, targetDataCenter);
        Collections.shuffle(targets);
        for (StorageGroup target: targets) {
          if (excluded.contains(target)) {
            continue;
          }
          final PendingMove pm = source.addPendingMove(fullPath, db, target);
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
        final List<StorageGroup> targets = storages.getTargetStorages(t, targetDataCenter);
        int total = 0;
        for (StorageGroup target: targets) {
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

  class Fetcher extends Thread {

    private final Processor processor;

    Fetcher(Processor processor) {
      super("ZoneMover-Fetcher");
      this.processor = processor;
    }

    @Override
    public void run() {
      ZoneReplicationCoordinator.FileState fileState = null;
      while (true) {
        try {
          fileState = coordinator.getNextFinishedFile();
          processor.processFileBlocks(fileState.getFilePath(), fileState.getFileStatus(),
              fileState.getRule(), result);
        } catch (NoSuchElementException e) {
          LOG.warn("No more files!", e);
          break;
        } catch (Exception e) {
          LOG.warn("Fetcher encountered the exception!", e);
        }
      }
    }
  }

  /* Monitor records in zookeeper and sync the records */
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
            updatePathRuleMap(driver, namenode.getAuthority(),
                zoneMoverTrigger);
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
      zoneMoverTrigger.updatePaths(Cli.getPaths(pathRuleMap));
    }
  }

  static class Cli extends Configured implements Tool {
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
          new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8));
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
        } else {
          paths = getPaths(commandLine);
          rule = getRule(commandLine);
        }

        if (commandLine.hasOption("monitorByTrigger")) {
          ZoneMoverTrigger zoneMoverTrigger =
              new ZoneMoverKafkaTrigger(conf, paths, namenode);
          if (rule != null) {
            return run(zoneMoverTrigger, conf, namenode, paths, rule);
          } else {
            return run(zoneMoverTrigger, conf, namenode, paths, pathRuleMap);
          }
        } else if (commandLine.hasOption("monitor")) {
          //noinspection InfiniteLoopStatement
          while (true) {
            startTime = Time.monotonicNow();
            run(conf, namenode, paths, rule);
            LOG.info("ZoneMover took "
                + StringUtils.formatTime(Time.monotonicNow() - startTime));
            //noinspection BusyWait
            Thread.sleep(monitorCheckInterval);
          }
        } else {
          if (rule != null) {
            return run(conf, namenode, paths, rule);
          } else {
            return run(conf, namenode, paths, pathRuleMap);
          }

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
        LOG.info("ZoneMover took "
            + StringUtils.formatTime(Time.monotonicNow() - startTime));
      }
    }

    /**
     * Run with prepared arguments.
     */
    int run(Configuration conf, URI namenode,
        List<Path> paths, ReplicationRule rule)
        throws IOException, InterruptedException {
      return ZoneMover.run(conf, namenode, paths, rule);
    }

    /**
     * Run with prepared arguments.
     */
    int run(Configuration conf, URI namenode, List<Path> paths,
            Map<String, ReplicationRule> pathRuleMap)
        throws IOException, InterruptedException {
      return ZoneMover.run(conf, namenode, paths, pathRuleMap);
    }

    /**
     * Run with ZoneMoverTrigger for monitorByTrigger mode
     */
    int run(ZoneMoverTrigger zoneMoverTrigger, Configuration conf,
            URI namenode, List<Path> paths, ReplicationRule rule)
        throws IOException {
      return ZoneMover.run(zoneMoverTrigger, conf, namenode, paths, rule, false);
    }

    /**
     * Run with ZoneMoverTrigger for monitorByTrigger mode
     */
    int run(ZoneMoverTrigger zoneMoverTrigger, Configuration conf,
            URI namenode, List<Path> paths, Map<String, ReplicationRule> pathRuleMap)
        throws IOException, InterruptedException {
      return ZoneMover.run(zoneMoverTrigger, conf, namenode, paths, pathRuleMap, false);
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