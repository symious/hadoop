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
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.balancer.ExitStatus;
import org.apache.hadoop.hdfs.server.balancer.NameNodeConnector;
import org.apache.hadoop.hdfs.server.mover.Mover;
import org.apache.hadoop.hdfs.server.zoneservice.metrics.ZoneMoverMetrics;
import org.apache.hadoop.hdfs.server.zoneservice.metrics.ZoneProgressTracker;
import org.apache.hadoop.hdfs.server.zoneservice.metrics.ZoneServiceMetrics;
import org.apache.hadoop.hdfs.server.zoneservice.utils.MigrationDataCenters;
import org.apache.hadoop.hdfs.server.zoneservice.utils.RunMode;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/** The ZoneMigration is a tool that supports migrating replicas of blocks
 * between datacenters when migrate DC.
 */
public class ZoneMigration extends ZoneMover {
  public static final Logger LOG = LoggerFactory.getLogger(ZoneMigration.class);
  private static final String ID_PATH_PREFIX = "/system/zonemigration.id";
  protected RunMode runMode;
  private boolean allowChangeReplication;
  private MigrationRuleMap migrationRuleMap;
  private String sourceDC;
  private String targetDC;
  private boolean isDecrease = false;
  // Initialize ZoneMover Metrics.
  protected static ZoneMoverMetrics zoneMoverMetrics = ZoneMoverMetrics.create();
  protected ZoneMoverHttpServer httpServer;

  public ZoneMigration(NameNodeConnector nnc,
      Configuration conf, ReplicationRule rule,
      AtomicInteger retryCount, boolean changeReplica,
      String sourceDC, String targetDC, boolean isDecrease, RunMode runMode,
      boolean fromZS) throws IOException {
    super(nnc, conf, rule, retryCount, true, fromZS);
    initZoneMigration(conf, changeReplica, sourceDC, targetDC, isDecrease, runMode,
        null, null,fromZS);
  }

  public ZoneMigration(NameNodeConnector nnc,
      Configuration conf, ReplicationRule rule,
      AtomicInteger retryCount, boolean changeReplica,
      String sourceDC, String targetDC, boolean isDecrease, RunMode runMode,
      ZoneMoverTrigger zoneMoverTrigger) throws IOException {
    super(nnc, conf, rule, retryCount, true, false);
    initZoneMigration(conf, changeReplica, sourceDC, targetDC, isDecrease, runMode,
        zoneMoverTrigger, nnc.getNameNodeUri(), false);
  }

  public ZoneMigration(NameNodeConnector nnc,
      Configuration conf, ReplicationRule rule, AtomicInteger retryCount,
      boolean changeReplica, RunMode runMode, ZoneMoverTrigger zoneMoverTrigger,
      boolean fromZS) throws IOException {
    super(nnc, conf, rule, retryCount, true, fromZS);
    initZoneMigration(conf, changeReplica, "", "", false, runMode,
        zoneMoverTrigger, nnc.getNameNodeUri(), fromZS);
  }

  void initZoneMigration(Configuration conf, boolean changeReplica,
      String sourceDC, String targetDC, boolean isDecrease, RunMode runMode,
      ZoneMoverTrigger zoneMoverTrigger, URI namenode, boolean fromZS)
      throws IOException {
    super.init(conf);
    this.allowChangeReplication = changeReplica;
    this.sourceDC = sourceDC;
    this.targetDC = targetDC;
    this.isDecrease = isDecrease;
    this.migrationRuleMap = new MigrationRuleMap(conf);
    this.runMode = runMode;
    setEnableMigrationCluster(true);

    if (this.runMode.equals(RunMode.MONITOR)) {
      createTriggerRuleMapUpdater(namenode, zoneMoverTrigger);
      if (!fromZS) {
        // Metrics only enable in monitor mode and not from zone service called.
        startHttpServer(conf);
      }
    }
  }

  private void startHttpServer(final Configuration conf) throws IOException {
    httpServer = new ZoneMoverHttpServer(conf, getHttpServerBindAddress(conf));
    httpServer.start();
  }

  /**
   * HTTP server address for binding the endpoint. This method is
   * for use by the ZoneMover and its derivatives. It may return
   * a different address than the one that should be used by clients to
   * connect to the ZoneMover. See
   * {@link DFSConfigKeys#DFS_ZONEMOVER_HTTP_BIND_HOST_KEY}
   *
   * @param conf configuration of zone mover
   * @return return the http bind address of zone mover
   */
  protected InetSocketAddress getHttpServerBindAddress(Configuration conf) {
    InetSocketAddress bindAddress = getHttpAddress(conf);

    // If DFS_ZONEMOVER_HTTP_BIND_HOST_KEY exists then it overrides the
    // host name portion of DFS_ZONEMOVER_HTTP_ADDRESS_KEY.
    final String bindHost = conf.getTrimmed(DFSConfigKeys.DFS_ZONEMOVER_HTTP_BIND_HOST_KEY);
    if (bindHost != null && !bindHost.isEmpty()) {
      bindAddress = new InetSocketAddress(bindHost, bindAddress.getPort());
    }

    return bindAddress;
  }

  /** @return the ZoneMover HTTP address. */
  public static InetSocketAddress getHttpAddress(Configuration conf) {
    return  NetUtils.createSocketAddr(
        conf.getTrimmed(DFSConfigKeys.DFS_ZONEMOVER_HTTP_ADDRESS_KEY,
            DFSConfigKeys.DFS_ZONEMOVER_HTTP_ADDRESS_DEFAULT));
  }

  @Override
  protected Processor initProcessor() {
    return new ProcessorWithMigration();
  }

  @Override
  protected Fetcher initFetcher(Processor processor) {
    return new FetcherWithPreMigration(processor);
  }

  public static int runWithBatch(Configuration conf, URI namenode,
      List<Path> paths, ReplicationRule rule, String sourceDC,
      String targetDC, boolean isDecrease, boolean changeReplica, boolean fromZS)
      throws IOException, InterruptedException {
    ZoneProgressTracker.startCountingInitTime();
    if (rule != null) {
      checkDataCenterValues(conf, rule, null);
      LOG.info("Start to apply rule: " + rule + " to namenode:"
          + namenode + ", path: " + paths);
    }

    if (paths.isEmpty()) {
      ZoneProgressTracker.finishCountingInitTimeAndLog();
      return ExitStatus.SUCCESS.getExitCode();
    }

    NameNodeConnector nnc = null;
    ZoneMigration zm = null;
    // retryCount starts from 0 and ends at retryMaxAttempts
    AtomicInteger retryCount = new AtomicInteger(0);
    final long sleepTime = calculateSleepTime(conf);
    final boolean exitEvenHasProgress = conf.getBoolean(
        DFSConfigKeys.DFS_ZONEMOVER_EXIT_EVEN_HAS_PROGRESS,
        DFSConfigKeys.DFS_ZONEMOVER_EXIT_EVEN_HAS_PROGRESS_DEFAULT);

    try {
      // Set maxNotChangedIterations to 1 as ZoneMigration does not need to loop
      nnc = new NameNodeConnector(ZoneMigration.class.getSimpleName(),
          namenode, getIdPath(RunMode.BATCH), paths, conf, 1);
      nnc.getKeyManager().startBlockKeyUpdater();

      zm = new ZoneMigration(nnc, conf, rule, retryCount, changeReplica,
          sourceDC, targetDC, isDecrease, RunMode.BATCH, fromZS);
      int round = 0;

      ZoneProgressTracker.finishCountingInitTimeAndLog();

      while (true) {
        round += 1;
        LOG.info("Start round " + round + " ...");
        final ExitStatus r = zm.run();
        if (r == ExitStatus.SUCCESS) {
          break;
        } else if (r != ExitStatus.IN_PROGRESS) {
          if (r == ExitStatus.NO_MOVE_PROGRESS) {
            System.err.println("ZoneMigration Failed to move some blocks after "
                + zm.retryMaxAttempts + " retries. Exiting...");
          } else if (r == ExitStatus.NO_MOVE_BLOCK) {
            System.err.println("ZoneMigration Some blocks can't be moved. Exiting...");
          } else {
            System.err.println("ZoneMigration failed. Exiting with status " + r + "... ");
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
          LOG.info("ZoneMigration has progress in this round. Exiting ... ");
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

  public static int runWithMonitorByTrigger(ZoneMoverTrigger zoneMoverTrigger,
      Configuration conf, URI namenode, List<Path> paths, ReplicationRule rule,
      String sourceDC, String targetDC, boolean isDecrease, boolean changeReplica)
      throws IOException {
    if (paths.isEmpty()) {
      LOG.warn("No path to run monitor");
      return ExitStatus.SUCCESS.getExitCode();
    }

    if (rule != null) {
      checkDataCenterValues(conf, rule, null);
      LOG.info("Will apply rule: {} to namenode: {} for path: {}", rule, namenode, paths);
    }

    NameNodeConnector nnc = null;
    ZoneMigration zm = null;
    String ns = namenode.getAuthority();
    try {
      LOG.info("Initializing NameNodeConnector");
      nnc = new NameNodeConnector(ZoneMigration.class.getSimpleName(),
          namenode, getIdPath(RunMode.MONITOR), paths, conf, 1);
      nnc.getKeyManager().startBlockKeyUpdater();

      DefaultMetricsSystem.initialize("ZoneMover");
      zm = new ZoneMigration(nnc, conf, rule, new AtomicInteger(0), changeReplica,
          sourceDC, targetDC, isDecrease, RunMode.MONITOR, zoneMoverTrigger);

      while (zoneMoverTrigger.hasNext()) {
        try {
          Pair<ConsumerRecord<String, String>, String> curRecord = zoneMoverTrigger.getNextRecord();
          // process the path
          String curPath = curRecord.getRight();
          LOG.debug("Start to monitor process path: {}", curPath);
          long start = Time.now();
          ExitStatus exitStatus = zm.run(curPath);
          if (exitStatus != ExitStatus.SUCCESS) {
            zoneMoverMetrics.addFailTotalMove(Time.now() - start);
            LOG.warn("Failed to monitor process path fail: {}", curPath);
          } else {
            zoneMoverMetrics.addSuccessTotalMove(Time.now() - start);
            ConsumerRecord<String, String> record = curRecord.getLeft();
            zoneMoverTrigger.saveOffsetToZookeeper(record, ns, zoneMoverTrigger.getGroupId(),
                zoneMoverMetrics);
          }
        } catch (IllegalArgumentException e) {
          LOG.warn("Failed to monitor process path fail: {}", e.toString());
        } catch (InterruptedException e) {
          return ExitStatus.INTERRUPTED.getExitCode();
        }
      }
    } finally {
      if (nnc != null) {
        IOUtils.cleanupWithLogger(LOG, nnc);
      }
      if (zm != null) {
        zm.shutdown();
      }
      if (zoneMoverTrigger != null) {
        zoneMoverTrigger.shutdown();
      }
    }
    return ExitStatus.SUCCESS.getExitCode();
  }

  public static int run(Configuration conf, URI namenode,
      Map<String, ReplicationRule> replicationRuleMap) throws IOException {
    List<Path> paths = ZoneMover.Cli.getPaths(replicationRuleMap);
    ZoneMoverTrigger zoneMoverTrigger =
        new ZoneMoverKafkaTrigger(conf, paths, namenode);
    return ZoneMigration.runWithMonitorFromZoneService(zoneMoverTrigger, conf, namenode,
        paths, null);
  }

  /**
   * Run batch mode from ZoneService.
   */
  public static int run(Configuration conf, URI namenode, List<Path> paths)
      throws IOException, InterruptedException {
    return ZoneMigration.runWithBatch(conf, namenode, paths, null, "",
        "", false, true, true);
  }

  /**
   * Run monitorByTrigger mode from ZoneService.
   */
  public static int runWithMonitorFromZoneService(ZoneMoverTrigger zoneMoverTrigger,
      Configuration conf, URI namenode, List<Path> paths, ReplicationRule rule)
      throws IOException {

    if (rule != null) {
      checkDataCenterValues(conf, rule, null);
      LOG.info("Will apply rule: {} to namenode: {} for path: {} from zone service", rule,
          namenode, paths);
    }

    NameNodeConnector nnc = null;
    ZoneMigration zm = null;
    String ns = namenode.getAuthority();
    try {
      LOG.info("Initializing NameNodeConnector from zone service");
      nnc = new NameNodeConnector(ZoneMigration.class.getSimpleName(),
          namenode, getIdPath(RunMode.MONITOR), paths, conf, 1);
      nnc.getKeyManager().startBlockKeyUpdater();

      zm = new ZoneMigration(nnc, conf, rule, new AtomicInteger(0),
          true, RunMode.MONITOR, zoneMoverTrigger, true);

      // Initialize ZoneService Metrics.
      ZoneServiceMetrics zoneServiceMetrics = ZoneService.getMetrics();

      while (zoneMoverTrigger.hasNext()) {
        try {
          Pair<ConsumerRecord<String, String>, String> curRecord = zoneMoverTrigger.getNextRecord();
          // process the path
          String curPath = curRecord.getRight();
          LOG.debug("Start to monitor process path: {}", curPath);
          ExitStatus exitStatus = zm.run(curPath);
          if (exitStatus != ExitStatus.SUCCESS) {
            zoneServiceMetrics.incrFailMoveCount();
            zoneServiceMetrics.incrNSMonitorFailMoveCount(ns);
            LOG.warn("Failed to monitor process path fail: {}", curPath);
          } else {
            zoneServiceMetrics.incrNSMonitorSuccessMoveCount(ns);
            zoneServiceMetrics.incrSuccessMoveCount();
            ConsumerRecord<String, String> record = curRecord.getLeft();
            zoneMoverTrigger.saveOffsetToZookeeperForZS(record, ns, zoneMoverTrigger.getGroupId(),
                zoneServiceMetrics);

          }
        } catch (IllegalArgumentException e) {
          LOG.warn("Failed to monitor process path fail: {}", e.toString());
        } catch (InterruptedException e) {
          return ExitStatus.INTERRUPTED.getExitCode();
        }
      }
    } finally {
      if (nnc != null) {
        IOUtils.cleanupWithLogger(LOG, nnc);
      }
      if (zm != null) {
        zm.shutdown();
      }
      if (zoneMoverTrigger != null) {
        zoneMoverTrigger.shutdown();
      }
    }
    return ExitStatus.SUCCESS.getExitCode();
  }

  void createTriggerRuleMapUpdater(URI namenode, ZoneMoverTrigger zoneMoverTrigger)
      throws IOException {
    // Create MapUpdater thread to sync the Monitor records in zookeeper.
    String threadName = "ZoneMigration-MapUpdater" + namenode.getAuthority();
    try {
      MapUpdater mapUpdater = new MapUpdater(namenode, zoneMoverTrigger.getStoreDriver(),
          zoneMoverTrigger);
      Thread mapUpdaterThread = new Thread(mapUpdater, threadName);
      mapUpdaterThread.start();
      LOG.info("{} created.", threadName);
    } catch (Exception e) {
      throw new IOException("Failed to create " + threadName, e);
    }
  }

  @Override
  ExitStatus run() {
    try {
      return processor.processPath().getExitStatus();
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
  
  class ProcessorWithMigration extends Processor {
    @Override
    protected void processPath(String fullPath, ReplicationRule rule, Mover.Result result,
        MigrationDataCenters dc) {
      LOG.debug("Processing path: {}, mode: {}", fullPath, runMode);
      processPath(fullPath, rule, result, dc, true);
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
        preMigrationFile(fullPath, (HdfsLocatedFileStatus) status, rule, result);
      }
    }

    /**
     * Generate the file rule
     * If the file need to be pre-migrated,
     * will pre-migrate it and put into preMigrationFileQueue.
     * */
    private void preMigrationFile(String fullPath,
        HdfsLocatedFileStatus status, ReplicationRule rule,
        Mover.Result result) {
      LOG.debug("Processing file: {}, mode: {} {}", fullPath, runMode, isFromZS() ?
          "from zs." : String.format("from %s to %s", sourceDC, targetDC));

      final LocatedBlocks locatedBlocks = status.getLocatedBlocks();
      if (status.getLen() == 0) {
        LOG.debug("Skip empty file: {}", fullPath);
        ZoneProgressTracker.incrFileCount();
        return;
      }

      if (!locatedBlocks.isLastBlockComplete()) {
        LOG.debug("Skip uncompleted file: {}", fullPath);
        ZoneProgressTracker.incrFileCount();
        return;
      }

      LocatedBlock firstBlock = locatedBlocks.get(0);
      Map<String, Short> blockDistribution = getBlockDistribution(firstBlock);
      if (blockDistribution.size() == 0) {
        LOG.error("There are no replicas for the missing block: {} and file: {}",
            firstBlock, fullPath);
        ZoneProgressTracker.incrFileCount();
        return;
      }

      if (status.getErasureCodingPolicy() != null) {
        if (isFromZS()) {
          LOG.debug("No need to process data for ec file: {} from zs", fullPath);
          ZoneProgressTracker.incrFileCount();
          return;
        }
        LOG.debug("Process data for ec file: {}", fullPath);
        if (!firstBlock.isStriped()) {
          LOG.debug("No need to process data for non ec file: {}", fullPath);
          ZoneProgressTracker.incrFileCount();
          return;
        }
        processECFile(fullPath, status, result, sourceDC, targetDC);
        return;
      }

      ReplicationRule appliedRule = rule;
      ReplicationRule dis = ReplicationRule.parseFromMap(blockDistribution);
      if (appliedRule == null) {
        // If not set rule, it is generated from migrationRuleMap.
        if (isFromZS()) {
          appliedRule = migrationRuleMap.getRuleFromDistributionWithZS(dis, status.getReplication(),
              getValidDataCenters());
        } else {
          appliedRule = migrationRuleMap.getRuleFromDistribution(dis, status.getReplication(),
              sourceDC, targetDC, isDecrease);
        }
      }
      if (appliedRule == null) {
        LOG.debug("No need to process file: {} that are invalid rule by {}.", fullPath,
            runMode.getName());
        ZoneProgressTracker.incrFileCount();
        return;
      }

      if (appliedRule.getReplica() != status.getReplication() && !allowChangeReplication) {
        LOG.warn("Ignore replica inconsistency for file: {} and appliedReplica: {}, " +
                "oldReplica :{}", fullPath, appliedRule.getReplica(), status.getReplication());
        ZoneProgressTracker.incrFileCount();
        return;
      }

      // Queue file here to avoid multiple incr file
      ZoneProgressTracker.queueFile(fullPath);

      LOG.info("Will apply the data rule from {} to {} for file: {} by {}.", dis,
          appliedRule, fullPath, runMode.getName());

      // If two replicas are required to migrate, the tool will migrate one replica first
      // then the rest replica can copy from the migrated replica directly.
      Set<String> dataCenters = dis.getDatacenters();
      if (appliedRule.getReplica(targetDC) > 1
          && !dataCenters.contains(targetDC)
          && dis.getReplica() == status.getReplication()
          && dis.getReplica(sourceDC) > 1
          && enablePreMigration && !isFromZS()) {
        blockDistribution.put(sourceDC, (short) (blockDistribution.get(sourceDC) - 1));
        blockDistribution.put(targetDC, (short) 1);
        ReplicationRule preRule = ReplicationRule.parseFromMap(blockDistribution);
        LOG.info("Will pre-migrate 1 replica from {} to {} using preRule {} for {} by {}.",
            sourceDC, targetDC, preRule, fullPath, runMode.getName());
        try {
          processFileBlocks(fullPath, status, preRule, result, true);
          preMigrationFileQueue.put(new PreMigrationFile(fullPath, preRule, appliedRule));
        } catch (InterruptedException e) {
          processFile(fullPath, status, appliedRule, result);
          LOG.warn("Adding pre-migration file {} to the pre-migration queue is interrupted.",
              fullPath);
        }
      } else {
        processFile(fullPath, status, appliedRule, result);
      }
    }

    @Override
    protected void processFile(String fullPath,
        HdfsLocatedFileStatus status, ReplicationRule appliedRule,
        Mover.Result result) {
      LOG.debug("Processing file: {}, appliedRule: {}", fullPath, appliedRule);
      final LocatedBlocks locatedBlocks = status.getLocatedBlocks();
      // If the actual number of replicas is inconsistent with the rule's replicas,
      // first call setReplication.
      if (appliedRule.getReplica() != status.getReplication()) {
        if (allowChangeReplication) {
          try {
            long startRpcTime = Time.monotonicNow();
            LOG.debug("Before set replication: distribution is {}, appliedRule is {} for file: {}",
                getBlockDistribution(status.getLocatedBlocks().get(0)), appliedRule, fullPath);
            dfs.setReplication(fullPath, appliedRule.getReplica());
            ZoneProgressTracker.addSetReplicationTime(Time.monotonicNow() - startRpcTime);
            coordinator.addFile(fullPath, appliedRule,
                appliedRule.getReplica() - status.getReplication(),
                locatedBlocks.getLocatedBlocks().size());
          } catch (IOException e) {
            LOG.warn("Set replication fails for file: {}", fullPath, e);
            result.setRetryFailed();
          }
        } else {
          LOG.warn("Ignore replica not consistent for file: {}", fullPath);
          ZoneProgressTracker.dequeueFile(fullPath);
        }
      } else {
        processFileBlocks(fullPath, status, appliedRule, result, false);
        ZoneProgressTracker.dequeueFile(fullPath);
      }
    }

    /**
     * Process EC file, move all the internal blocks from sourceDC to targetDC.
     * */
    private void processECFile(String fullPath, HdfsLocatedFileStatus status,
        Mover.Result result, String sourceDC, String targetDC) {
      final LocatedBlocks locatedBlocks = status.getLocatedBlocks();
      final ErasureCodingPolicy erasureCodingPolicy = status.getErasureCodingPolicy();
      int n = locatedBlocks.locatedBlockCount();
      ZoneProgressTracker.queueFile(fullPath);
      for (int i = 0; i < n; i++) {
        List<ZoneMoveItem> moveItems = new ArrayList<>();
        LocatedBlock block = locatedBlocks.get(i);
        Map<String, Short> distribution = getBlockDistribution(block);
        if (distribution.containsKey(sourceDC)) {
          LOG.info("Block: {} will move from {} to {} for ec file: {}", block.getBlock(),
              sourceDC, targetDC, fullPath);
          moveItems.add(new ZoneMoveItem(sourceDC, targetDC, distribution.get(sourceDC)));
          if (scheduleMoves4Block(fullPath, block, moveItems, erasureCodingPolicy)) {
            result.setNoBlockMoved(false);
          } else {
            result.updateHasRemaining(true);
          }
        }
      }
      ZoneProgressTracker.dequeueFile(fullPath);
    }
  }

  void shutdown() {
    super.shutdown();
    zoneMoverMetrics.shutdown();
    if (httpServer != null) {
      try {
        httpServer.stop();
      } catch (Exception e) {
        LOG.error("Exception while stopping httpserver", e);
      }
    }
  }

  class FetcherWithPreMigration extends Fetcher {
    FetcherWithPreMigration(Processor processor) {
      super(processor, "ZoneMover-Fetcher-PreMigration");
    }

    @Override
    public void run() {
      LOG.info("Fetcher with pre-migration is started.");
      long lastRecord = Time.monotonicNow();
      ZoneReplicationCoordinator.FileState fileState = null;
      while (true) {
        try {
          fileState = coordinator.getNextFinishedFile();
          String fullPath = fileState.getFilePath();
          HdfsLocatedFileStatus status = fileState.getFileStatus();
          if (!status.getLocatedBlocks().isLastBlockComplete()) {
            LOG.debug("Skip uncompleted file: " + fullPath);
            continue;
          }
          processor.processFileBlocks(fullPath,status,
              fileState.getRule(), result, true);
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

  static class Cli extends Configured implements Tool {
    private static final String USAGE = "Usage: hdfs zonemigration"
        + "\n\t[-namespace <namespace>]\tthe namespace to apply the rule"
        + "\n\t[-path <path>]\tthe path to apply the rule"
        + "\n\t[-pathFile <pathFile>]\t the file contains paths to apply the rule"
        + "\n\t[-rule <rule>]\tthe replication rule"
        + "\n\t[-sourceDC <sourceDC>]\tthe migration source idc"
        + "\n\t[-targetDC <targetDC>]\tthe migration target idc"
        + "\n\t[-isDecrease]\twhether to decrease the replication"
        + "\n\t[-changeReplica]\twhether to allow change the replication"
        + "\n\t[-monitorByTrigger]\ttenable monitor mode with trigger";

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
      options.addOptionGroup(pathGroup);

      option = new Option("rule", "rule", true, "the replication rule");
      options.addOption(option);

      option = new Option("sourceDC", "sourceDC", true,
          "the migration source idc");
      options.addOption(option);

      option = new Option("targetDC", "targetDC", true,
          "the migration target idc");
      options.addOption(option);

      option = new Option("isDecrease", "isDecrease", false,
          "whether to decrease the replication");
      options.addOption(option);

      option = new Option("changeReplica", "-changeReplica", false,
          "whether to allow change the replication");
      options.addOption(option);

      option = new Option(null, "monitorByTrigger", false,
          "enable monitor by trigger");
      options.addOption(option);
      return options;
    }

    private static void additionalOptionsCheck(CommandLine line)
        throws IllegalArgumentException {
      // As commons-cli 1.2 does not support adding one option
      // to two OptionGroups, we need to check this in a trick way.
      boolean hasRule = line.hasOption("rule");
      boolean hasSourceAndTarget = line.hasOption("sourceDC") && line.hasOption("targetDC");
      if (!hasRule && !hasSourceAndTarget) {
        throw new IllegalArgumentException("must specify either '-rule' " +
            "or both '-sourceDC' and '-targetDC'.");
      }
    }

    @Override
    public int run(String[] args) {
      long startTime = Time.monotonicNow();
      final Configuration conf = getConf();
      final Options options = buildCliOptions();
      CommandLineParser parser = new GnuParser();
      try {
        CommandLine commandLine = parser.parse(options, args, true);
        additionalOptionsCheck(commandLine);
        URI namenode = getNamespaceUri(commandLine, conf);
        List<Path> paths = ZoneMover.Cli.getPaths(commandLine);
        ReplicationRule rule = null;
        if (commandLine.hasOption("rule")) {
          rule = ZoneMover.Cli.getRule(commandLine);
        }
        String sourceDC = commandLine.getOptionValue("sourceDC");
        String targetDC = commandLine.getOptionValue("targetDC");
        boolean isDecrease = commandLine.hasOption("isDecrease");
        boolean changeReplica = commandLine.hasOption("changeReplica");
        boolean isMonitor = commandLine.hasOption("monitorByTrigger");
        LOG.info("ZoneMigration start with mode: {}, rule:{}, sourceDC: {}, targetDC: {}, " +
            "isDecrease: {}, changeReplica: {}", isMonitor ?
                RunMode.MONITOR.getName() : RunMode.BATCH.getName(),rule, sourceDC, targetDC,
            isDecrease, changeReplica);

        // Default is batch mode.
        if (isMonitor) {
          ZoneMoverTrigger zoneMoverTrigger = new ZoneMoverKafkaTrigger(conf,
              paths, namenode, true);
          return run(zoneMoverTrigger, conf, namenode, paths, rule, sourceDC, targetDC,
              isDecrease, changeReplica);
        } else {
          return run(conf, namenode, paths, rule, sourceDC, targetDC, isDecrease, changeReplica);
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
      }
      finally {
        LOG.info("ZoneMigration took "
            + StringUtils.formatTime(Time.monotonicNow() - startTime));
      }
    }

    /**
     * Run with ZoneMoverTrigger for batch mode.
     */
    int run(Configuration conf, URI namenode, List<Path> paths, ReplicationRule rule,
        String sourceDC, String targetDC, boolean isDecrease, boolean changeReplica)
        throws IOException, InterruptedException {
      return ZoneMigration.runWithBatch(conf, namenode, paths, rule, sourceDC,
          targetDC, isDecrease, changeReplica, false);
    }

    /**
     * Run with ZoneMoverTrigger for monitorByTrigger mode.
     */
    int run(ZoneMoverTrigger zoneMoverTrigger, Configuration conf, URI namenode,
        List<Path> paths, ReplicationRule rule,String sourceDC, String targetDC,
        boolean isDecrease, boolean changeReplica) throws IOException {
      return ZoneMigration.runWithMonitorByTrigger(zoneMoverTrigger, conf, namenode, paths,
          rule, sourceDC, targetDC, isDecrease, changeReplica);
    }
  }

  public static void main(String[] args) {
    if (DFSUtil.parseHelpArgument(args, ZoneMigration.Cli.USAGE, System.out,
        true)) {
      System.exit(0);
    }

    try {
      System.exit(ToolRunner.run(new HdfsConfiguration(), new ZoneMigration.Cli(), args));
    } catch (Throwable e) {
      LOG.error("Exiting ZoneMigration due to an exception", e);
      System.exit(-1);
    }
  }
}
