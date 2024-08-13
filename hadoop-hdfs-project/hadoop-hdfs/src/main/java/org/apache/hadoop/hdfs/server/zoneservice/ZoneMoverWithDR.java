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
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.server.balancer.ExitStatus;
import org.apache.hadoop.hdfs.server.balancer.NameNodeConnector;
import org.apache.hadoop.hdfs.server.mover.Mover;
import org.apache.hadoop.hdfs.server.zoneservice.metrics.ZoneMoverMetrics;
import org.apache.hadoop.hdfs.server.zoneservice.metrics.ZoneProgressTracker;
import org.apache.hadoop.hdfs.server.zoneservice.utils.MigrationDataCenters;
import org.apache.hadoop.hdfs.server.zoneservice.utils.RunMode;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.util.Time.now;

public class ZoneMoverWithDR extends ZoneMover {

  public static final Logger LOG = LoggerFactory.getLogger(ZoneMoverWithDR.class);
  private static final String ID_PATH_PREFIX = "/system/zonemoverwithdr.id";
  private final RunMode runMode;
  private final boolean useAccessTime;
  private final boolean skipReplica;
  private final boolean skipEC;
  private final boolean skipCheckCold;
  private final long drColdDataThresholdMS;
  private Set<String> drDataCenters;
  private Map<Short, ReplicationRule> drReplicationRuleForColdData;
  private Map<Short, ReplicationRule> drStripedBlockRule;
  // Initialize ZoneMover Metrics.
  protected static ZoneMoverMetrics zoneMoverMetrics = ZoneMoverMetrics.create();
  protected ZoneMoverHttpServer httpServer;

  public ZoneMoverWithDR(NameNodeConnector nnc, Configuration conf, AtomicInteger retryCount,
      RunMode runMode, boolean useAccessTime, boolean skipReplica, boolean skipEC,
      boolean skipCheckCold)
      throws IOException {
    super(nnc, conf, retryCount, true, false);
    this.runMode = runMode;
    drColdDataThresholdMS = conf.getLong(
        DFSConfigKeys.DFS_NAMENODE_DR_COLD_DATA_THRESHOLD_MS_KEY,
        DFSConfigKeys.DFS_NAMENODE_DR_COLD_DATA_THRESHOLD_MS_DEFAULT);
    setDrDataCenters(new HashSet<>(StringUtils.getTrimmedStringCollection(
        conf.get(DFSConfigKeys.DFS_NAMENODE_DR_DATACENTERS_KEY))));
    setDrReplicationRuleForColdData(StringUtils.getTrimmedStringCollection(
        conf.get(DFSConfigKeys.DFS_NAMENODE_DR_REPLICATION_RULE_COLD_DATA_KEY), ";"));
    setDrStripedBlockRule(StringUtils.getTrimmedStringCollection(
        conf.get(DFSConfigKeys.DFS_NAMENODE_DR_STRIPED_BLOCK_RULE_KEY), ";"));
    setEnableDR(true);
    this.useAccessTime = useAccessTime;
    this.skipReplica = skipReplica;
    this.skipEC = skipEC;
    this.skipCheckCold = skipCheckCold;
    startHttpServer(conf);
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

  private void setDrDataCenters(Set<String> drDataCenters) {
    Preconditions.checkArgument(drDataCenters.size() == 2,
        "%s should be set with 2 IDCs for DR",
        DFSConfigKeys.DFS_NAMENODE_DR_DATACENTERS_KEY);
    this.drDataCenters = drDataCenters;
  }

  private void setDrReplicationRuleForColdData(Collection<String> replicaRuleCollections)
      throws IOException {
    Map<Short, ReplicationRule> replicationRules = new HashMap<>();
    for (String strRule : replicaRuleCollections) {
      String[] keyValue = strRule.split("=");
      if (keyValue.length == 2) {
        Short replica = Short.valueOf(keyValue[0].trim());
        String rule = keyValue[1].trim();
        ReplicationRule replicationRule = ReplicationRule.parseFromString(rule);
        // Verify the validity of IDC.
        if (drDataCenters.containsAll(replicationRule.getDatacenters())) {
          replicationRules.put(replica, replicationRule);
        } else {
          String msg = String.format("Invalid cold data replication rule: %s for DR.",
              strRule);
          LOG.error(msg);
          throw new IOException(msg);
        }
      }
    }

    Preconditions.checkArgument(replicationRules.containsKey((short) 3),
        "%s least should contain 3 replica corresponding " +
            "replication rule for DR",
        DFSConfigKeys.DFS_NAMENODE_DR_REPLICATION_RULE_COLD_DATA_KEY);
    drReplicationRuleForColdData = replicationRules;
  }

  public void setDrStripedBlockRule(Collection<String> stripedBlockRuleCollections)
      throws IOException {
    Map<Short, ReplicationRule> stripedBlockRules = new HashMap<>();
    for (String strRule : stripedBlockRuleCollections) {
      String[] keyValue = strRule.split("=");
      if (keyValue.length == 2) {
        Short replica = Short.valueOf(keyValue[0].trim());
        String rule = keyValue[1].trim();
        ReplicationRule replicationRule = ReplicationRule.parseFromString(rule);
        // Verify the validity of IDC.
        if (drDataCenters.containsAll(replicationRule.getDatacenters())) {
          stripedBlockRules.put(replica, replicationRule);
        } else {
          String msg = String.format("Invalid striped block rule: %s for DR.",
              strRule);
          LOG.error(msg);
          throw new IOException(msg);
        }
      }
    }

    Preconditions.checkArgument(stripedBlockRules.containsKey((short) 9),
        "%s least should contain 9 replica corresponding " +
            "striped block rule for DR",
        DFSConfigKeys.DFS_NAMENODE_DR_STRIPED_BLOCK_RULE_KEY);
    drStripedBlockRule = stripedBlockRules;
  }

  @Override
  protected Processor initProcessor() {
    return new ProcessorWithDR();
  }

  static class Cli extends Configured implements Tool {
    private static final String USAGE = "Usage: hdfs zonemoverwithdr"
        + "\n\t[-namespace <namespace>]\tthe namespace to apply the dr rule"
        + "\n\t-path <path>\tthe path to apply the dr rule"
        + "\n\t-pathFile <pathFile>\t the file contains paths to apply the dr rule"
        + "\n\t-monitorByTrigger\tenable monitor mode with trigger"
        + "\n\t-cold\tenable cold mode"
        + "\n\t-useAccessTime\the definition of cold data determines whether to use " +
        "accesstime or modifiedtime"
        + "\n\t-skipEC\twhether to skip EC files"
        + "\n\t-skipReplica\twhether to skip replication files"
        + "\n\t-skipCheckCold\twhether to check the file is cold data";

    private static Options buildCliOptions() {
      Options options = new Options();
      Option option = new Option(
          null, "namespace", true, "the namespace to apply the rule");
      options.addOption(option);

      OptionGroup pathGroup = new OptionGroup();
      pathGroup.setRequired(true);
      option = new Option("path", "path", true,
          "the path to apply the rule");
      pathGroup.addOption(option);
      option = new Option("pathFile", "pathFile", true,
          "the file contains paths to apply the rule");
      pathGroup.addOption(option);
      options.addOptionGroup(pathGroup);

      option = new Option(null, "monitorByTrigger", false,
          "enable monitor by trigger");
      options.addOption(option);

      option = new Option(null, "cold", false, "enable cold mode");
      options.addOption(option);

      option = new Option(null, "useAccessTime", false, "the " +
          "definition of cold data determines whether to use accesstime or modifiedtime");
      options.addOption(option);

      option = new Option(null, "skipEC", false,
          "Whether to skip EC files");
      options.addOption(option);

      option = new Option(null, "skipReplica", false,
          "Whether to skip replication files");
      options.addOption(option);

      option = new Option(null, "skipCheckCold", false,
          "Whether to check the file is cold data");
      options.addOption(option);
      return options;
    }

    private static void additionalOptionsCheck(CommandLine line)
        throws IllegalArgumentException {
      // As commons-cli 1.2 does not support adding one option
      // to two OptionGroups, we need to check this in a trick way.
      if (!line.hasOption("-cold") && !line.hasOption("-monitorByTrigger")) {
        throw new IllegalArgumentException
            ("'-cold' and '-monitorByTrigger' must specify one.");
      }

      if (line.hasOption("-cold") && line.hasOption("-monitorByTrigger")) {
        throw new IllegalArgumentException
            ("'-cold' and '-monitorByTrigger' only support specify one.");
      }

      if (line.hasOption("-skipReplica") && line.hasOption("-skipEC")) {
        throw new IllegalArgumentException
            ("'The options '-skipReplica' and '-skipEC' cannot be specified together, " +
                "can either specify one or leave both unspecified.");
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
        boolean useAccessTime = commandLine.hasOption("-useAccessTime");
        boolean skipReplica = commandLine.hasOption("-skipReplica");
        boolean skipEC = commandLine.hasOption("-skipEC");
        boolean skipCheckCold = commandLine.hasOption("-skipCheckCold");
        List<Path> paths = ZoneMover.Cli.getPaths(commandLine);
        if (commandLine.hasOption("cold")) {
          return run(conf, namenode, paths, useAccessTime, skipReplica, skipEC, skipCheckCold);
        } else if (commandLine.hasOption("monitorByTrigger")) {
          ZoneMoverTrigger zoneMoverTrigger =
              new ZoneMoverKafkaTrigger(conf, paths, namenode, true);
          return run(zoneMoverTrigger, conf, namenode, paths, skipReplica, skipEC);
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
        LOG.info("ZoneMoverWithDR took {}.",
            StringUtils.formatTime(Time.monotonicNow() - startTime));
      }
      return 0;
    }

    /**
     * Run with ZoneMoverTrigger for cold mode.
     */
    int run(Configuration conf, URI namenode, List<Path> paths, boolean useAccessTime,
        boolean skipReplica, boolean skipEC, boolean skipCheckCold)
        throws IOException, InterruptedException {
      return ZoneMoverWithDR.runWithColdDataReplication(conf, namenode, paths, useAccessTime,
          skipReplica, skipEC, skipCheckCold);
    }

    /**
     * Run with ZoneMoverTrigger for monitorByTrigger mode.
     */
    int run(ZoneMoverTrigger zoneMoverTrigger, Configuration conf, URI namenode,
        List<Path> paths, boolean skipReplica, boolean skipEC) throws IOException {
      return ZoneMoverWithDR.runWithNewDataReplication(zoneMoverTrigger, conf, namenode, paths,
          skipReplica, skipEC);
    }
  }

  public static int runWithColdDataReplication(Configuration conf, URI namenode,
      List<Path> paths, boolean useAccessTime, boolean skipReplica, boolean skipEC,
      boolean skipCheckCold)
      throws IOException, InterruptedException {
    ZoneProgressTracker.startCountingInitTime();

    LOG.info("Start to apply dr cold data rule to namenode: {}, path: {}, useAccessTime: {}, " +
            "skipReplica: {}, skipEC: {}, skipCheckCold: {}", namenode, paths, useAccessTime,
        skipReplica, skipEC, skipCheckCold);
    if (paths.isEmpty()) {
      ZoneProgressTracker.finishCountingInitTimeAndLog();
      return ExitStatus.SUCCESS.getExitCode();
    }

    NameNodeConnector nnc = null;
    ZoneMoverWithDR zm = null;
    // retryCount starts from 0 and ends at retryMaxAttempts
    AtomicInteger retryCount = new AtomicInteger(0);
    final long sleepTime = calculateSleepTime(conf);
    final boolean exitEvenHasProgress = conf.getBoolean(
        DFSConfigKeys.DFS_ZONEMOVER_EXIT_EVEN_HAS_PROGRESS,
        DFSConfigKeys.DFS_ZONEMOVER_EXIT_EVEN_HAS_PROGRESS_DEFAULT);

    try {
      // Set maxNotChangedIterations to 1 as ZoneMoverWithDR does not need to loop
      nnc = new NameNodeConnector(ZoneMoverWithDR.class.getSimpleName(),
          namenode, getIdPath(RunMode.COLD), paths, conf, 1);
      nnc.getKeyManager().startBlockKeyUpdater();

      zm = new ZoneMoverWithDR(nnc, conf, retryCount, RunMode.COLD, useAccessTime,
          skipReplica, skipEC, skipCheckCold);
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

  public static int runWithNewDataReplication(ZoneMoverTrigger zoneMoverTrigger,
      Configuration conf, URI namenode, List<Path> paths, boolean skipReplica, boolean skipEC)
      throws IOException {
    if (paths.isEmpty()) {
      return ExitStatus.SUCCESS.getExitCode();
    }

    NameNodeConnector nnc = null;
    ZoneMoverWithDR zm = null;
    String ns = namenode.getAuthority();
    try {
      LOG.info("Initializing NameNodeConnector");
      nnc = new NameNodeConnector(ZoneMoverWithDR.class.getSimpleName(),
          namenode, getIdPath(RunMode.MONITOR), paths, conf, 1);
      nnc.getKeyManager().startBlockKeyUpdater();

      DefaultMetricsSystem.initialize("ZoneMover");
      zm = new ZoneMoverWithDR(nnc, conf, new AtomicInteger(0), RunMode.MONITOR,
          false, skipReplica, skipEC, false);
      zm.init(conf);

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

  static Path getIdPath(RunMode mode) {
    return new Path(String.format("%s.%s.%s.%s",
        ID_PATH_PREFIX,
        mode.toString().toLowerCase(),
        NetUtils.getLocalHostname(),
        Time.now()));
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

  class ProcessorWithDR extends Processor {
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
        preMigrationFile(fullPath, (HdfsLocatedFileStatus) status, result);
      }
    }

    /**
     * Generate the file rule
     * If the file need to be pre-migrated,
     * will pre-migrate it and put into preMigrationFileQueue
     * */
    private void preMigrationFile(String fullPath, HdfsLocatedFileStatus status,
        Mover.Result result) {
      LOG.debug("Processing file: {}, mode: {}", fullPath, runMode);

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

      LocatedBlock firstBlock = status.getLocatedBlocks().get(0);
      Map<String, Short> blockDistribution = getBlockDistribution(firstBlock);
      if (blockDistribution.isEmpty()) {
        LOG.error("There are no replicas for the missing block: {} by file: {}", firstBlock,
            fullPath);
        ZoneProgressTracker.incrFileCount();
        return;
      }

      if (!drDataCenters.containsAll(blockDistribution.keySet())) {
        LOG.error("Cannot generate valid replica rule for file: {}", fullPath);
        ZoneProgressTracker.incrFileCount();
        return;
      }

      short repl = status.getReplication();
      ReplicationRule appliedRule = null;

      if (status.getErasureCodingPolicy() != null) {
        if (skipEC) {
          LOG.debug("No need to process data for ec file: {}.", fullPath);
          ZoneProgressTracker.incrFileCount();
        } else {
          LOG.debug("Process data for ec file: {}.", fullPath);
          processECFile(fullPath, firstBlock, status, result);
        }
        return;
      }

      if (skipReplica) {
        LOG.debug("No need to process data for replication file: {}.", fullPath);
        ZoneProgressTracker.incrFileCount();
        return;
      }

      if (runMode.equals(RunMode.COLD)) {
        if (!skipCheckCold) {
          long time = useAccessTime ? status.getAccessTime() : status.getModificationTime();
          boolean isColdData = now() > time + drColdDataThresholdMS;
          if (!isColdData) {
            LOG.debug("No need to process file: {} that are not cold data.", fullPath);
            ZoneProgressTracker.incrFileCount();
            return;
          }
        }
        // The strategy for configuring cold data, such as:
        // 3 replicas (/YTL:2,/AT:1).
        // The replicaRulesForDR should be contained 3 replica rule.
        appliedRule = drReplicationRuleForColdData.get(repl);
      }

      if (appliedRule == null) {
        HashMap<String, Integer> dcMap = new HashMap<>();
        for (String dc : drDataCenters) {
          dcMap.put(dc, Integer.valueOf(blockDistribution.getOrDefault(dc, (short) 0)));
        }
        appliedRule = ReplicationRuleUtil.generateRuleForDR(dcMap, repl);
        if (appliedRule == null) {
          LOG.debug("No need to process file: {} that are invalid rule by {}.", fullPath,
              runMode.getName());
          ZoneProgressTracker.incrFileCount();
          return;
        }
      }

      ReplicationRule oldRule = ReplicationRule.parseFromMap(blockDistribution);
      LOG.info("Will apply the data rule from {} to {} for file: {} by {}.", oldRule,
          appliedRule, fullPath, runMode.getName());

      // If two replicas are required to migrate, the tool will migrate one replica first
      // then the rest replica can copy from the migrated replica directly.
      if (oldRule.getDatacenters().size() == 1) {
        String singleDc = oldRule.getDatacenters().iterator().next();
        if (repl - appliedRule.getReplica(singleDc) > 1 && enablePreMigration) {
          List<String> tmpDataCenters = new ArrayList<>(drDataCenters);
          tmpDataCenters.remove(singleDc);
          blockDistribution.put(singleDc, (short) (blockDistribution.get(singleDc) - 1));
          blockDistribution.put(tmpDataCenters.get(0), (short) 1);
          ReplicationRule preRule = ReplicationRule.parseFromMap(blockDistribution);
          LOG.info("Will pre-migrate 1 replica from {} to {} using preRule {} for {} by {}.",
              singleDc, tmpDataCenters.get(0), preRule, fullPath, runMode.getName());
          try {
            ZoneProgressTracker.queueFile(fullPath);
            processFileBlocks(fullPath, status, preRule, result, true);
            preMigrationFileQueue.put(new PreMigrationFile(fullPath, preRule, appliedRule));
          } catch (InterruptedException e) {
            ZoneProgressTracker.dequeueFile(fullPath);
            processFile(fullPath, status, appliedRule, result);
            LOG.warn("Adding pre-migration file {} to the pre-migration queue is interrupted.",
                fullPath);
          }
        } else {
          processFile(fullPath, status, appliedRule, result);
        }
      }
    }

    @Override
    protected void processFile(String fullPath,
        HdfsLocatedFileStatus status, ReplicationRule appliedRule,
        Mover.Result result) {
      processFileBlocks(fullPath, status, appliedRule, result, false);
    }

    protected void processFileBlocks(String fullPath, HdfsLocatedFileStatus status,
        ReplicationRule rule, Mover.Result result, boolean hasPreMigration) {
      LocatedBlocks locatedBlocks = status.getLocatedBlocks();
      int n = locatedBlocks.locatedBlockCount();
      ZoneProgressTracker.queueFile(fullPath);
      for (int i = 0; i < n; i++) {
        LocatedBlock block = locatedBlocks.get(i);
        Map<String, Short> distribution = getBlockDistribution(block);
        if (!drDataCenters.containsAll(distribution.keySet())) {
          LOG.debug("Block: {} cannot generate valid replica rule for file: {}", block, fullPath);
          continue;
        }

        ReplicationRule dis = ReplicationRule.parseFromMap(distribution);
        if (rule.equals(dis)) {
          LOG.debug("Block: {} rule: {} is expected will skip for file: {}", block, dis, fullPath);
          continue;
        }

        if (scheduleMoves4Block(fullPath, block, getZoneMoveItems(distribution, rule))) {
          result.setNoBlockMoved(false);
        } else {
          result.updateHasRemaining(true);
        }
      }
      ZoneProgressTracker.dequeueFile(fullPath);
    }


    /**
     * Process EC file.
     * */
    private void processECFile(String fullPath, LocatedBlock locatedBlock,
        HdfsLocatedFileStatus status, Mover.Result result) {
      if (!locatedBlock.isStriped()) {
        LOG.debug("No need to process data for non ec file: {}", fullPath);
        ZoneProgressTracker.incrFileCount();
        return;
      }
      final ErasureCodingPolicy ecPolicy = status.getErasureCodingPolicy();
      // Current `drStripedBlockRule` only configures RS-6-3.
      int totalBlockNum = ecPolicy.getNumParityUnits() + ecPolicy.getNumDataUnits();
      ReplicationRule rule = drStripedBlockRule.get((short) totalBlockNum);
      if (rule == null) {
        LOG.info("No need to process data for ec file: {}, because codec name: {} is not set.",
            fullPath, ecPolicy.getName());
        ZoneProgressTracker.incrFileCount();
        return;
      }
      final LocatedBlocks locatedBlocks = status.getLocatedBlocks();
      int n = locatedBlocks.locatedBlockCount();
      ZoneProgressTracker.queueFile(fullPath);
      for (int i = 0; i < n; i++) {
        LocatedBlock block = locatedBlocks.get(i);

        // Retrieve rule based on the total number of blocks in the striped block.
        LocatedStripedBlock stripedBlock = (LocatedStripedBlock) locatedBlock;
        int blockNumExpected = Math.min(ecPolicy.getNumDataUnits(),
            (int) ((stripedBlock.getBlockSize() - 1) / ecPolicy.getCellSize() + 1)) +
            ecPolicy.getNumParityUnits();

        // If the block group is full blocks, return the rule directly.
        // Otherwise, need to be generated new rule.
        if (blockNumExpected < totalBlockNum) {
          rule = ReplicationRuleUtil.generateStripedBlockRuleForDR(rule, blockNumExpected);
        }

        Map<String, Short> distribution = getBlockDistribution(block);

        if (!drDataCenters.containsAll(distribution.keySet())) {
          LOG.debug("Block: {} cannot generate valid striped rule for ec file: {}",
              block.getBlock(), fullPath);
          continue;
        }

        ReplicationRule dis = ReplicationRule.parseFromMap(distribution);
        if (rule.equals(dis)) {
          LOG.debug("Block: {} rule: {} is expected will skip for ec file: {}", block.getBlock(),
              dis, fullPath);
          continue;
        }

        LOG.info("Block: {} will apply the rule from {} to {} for ec file: {}", block.getBlock(),
            dis, rule, fullPath);
        if (scheduleMoves4Block(fullPath, block, getZoneMoveItems(distribution, rule),
            ecPolicy)) {
          result.setNoBlockMoved(false);
        } else {
          result.updateHasRemaining(true);
        }
      }
      ZoneProgressTracker.dequeueFile(fullPath);
    }
  }

  public static void main(String[] args) {
    if (DFSUtil.parseHelpArgument(args, ZoneMoverWithDR.Cli.USAGE, System.out,
        true)) {
      System.exit(0);
    }

    try {
      System.exit(
          ToolRunner.run(new HdfsConfiguration(), new ZoneMoverWithDR.Cli(), args));
    } catch (Throwable e) {
      LOG.error("Exiting ZoneMoverWithDR due to an exception", e);
      System.exit(-1);
    }
  }
}
