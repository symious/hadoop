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
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.balancer.ExitStatus;
import org.apache.hadoop.hdfs.server.mover.Mover;
import org.apache.hadoop.hdfs.server.zoneservice.metrics.ZoneMoverMetrics;
import org.apache.hadoop.hdfs.server.zoneservice.metrics.ZoneProgressTracker;
import org.apache.hadoop.hdfs.server.zoneservice.utils.RunMode;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/** The ZoneMigration is a tool that supports migrating replicas of blocks
 * between datacenters when migrate DC.
 */
public class ZoneMigrationV2 extends ZoneMoverV2 {
  public static final Logger LOG = LoggerFactory.getLogger(ZoneMigration.class);
  private final boolean allowChangeReplication;
  private final MigrationRuleMap migrationRuleMap;
  private final String sourceDC;
  private final String targetDC;
  private final boolean isDecrease;

  public ZoneMigrationV2(Configuration conf, URI nameNode, List<Path> paths,
      RunMode runMode, ZoneMoverTrigger zoneMoverTrigger,
      ReplicationRule rule, boolean changeReplica,
      String sourceDC, String targetDC, boolean isDecrease) throws IOException {

    super(conf, nameNode, paths, runMode, zoneMoverTrigger, rule);

    this.allowChangeReplication = changeReplica;
    this.sourceDC = sourceDC;
    this.targetDC = targetDC;
    this.isDecrease = isDecrease;
    this.migrationRuleMap = new MigrationRuleMap(conf);
    this.enableMigrationDC = true;
  }

  @Override
  protected CoordinatorFetcher initCoordinatorFetcher(Configuration conf) {
    return new MigrationCoordinatorFetcher(conf);
  }

  /**
   * Start ZoneMover to migrate existing files in batch.
   */
  public static int runWithBatch(Configuration conf, URI namenode,
      List<Path> paths, ReplicationRule rule, String sourceDC,
      String targetDC, boolean isDecrease, boolean changeReplica)
      throws IOException {
    ZoneProgressTracker.startCountingInitTime();
    if (rule != null) {
      ZoneUtil.checkDataCenterValues(conf, rule, null);
      LOG.info("Start to apply rule: {} to namenode:{}, path: {}", rule,
          namenode, paths);
    }

    if (paths.isEmpty()) {
      ZoneProgressTracker.finishCountingInitTimeAndLog();
      return ExitStatus.SUCCESS.getExitCode();
    }

    ZoneMigrationV2 zm = null;
    try {
      zm = new ZoneMigrationV2(conf, namenode, paths, RunMode.BATCH, null,
          rule, changeReplica, sourceDC, targetDC, isDecrease);
      zm.start();
      ZoneProgressTracker.finishCountingInitTimeAndLog();

      // Migrating files.
      return zm.migratePaths().getExitCode();

    } finally {

      ZoneProgressTracker.checkForLeak();
      if (zm != null) {
        zm.shutdown();
      }
    }
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
      ZoneUtil.checkDataCenterValues(conf, rule, null);
      LOG.info("Will apply rule: {} to namenode: {} for path: {}", rule, namenode, paths);
    }

    ZoneMigrationV2 zm = null;
    try {
      zm = new ZoneMigrationV2(conf, namenode, paths, RunMode.MONITOR,
          zoneMoverTrigger, rule, changeReplica, sourceDC, targetDC, isDecrease);
      zm.startInTriggerMonitor();
    } finally {
      if (zm != null) {
        zm.shutdown();
      }
      if (zoneMoverTrigger != null) {
        zoneMoverTrigger.shutdown();
      }
    }
    return ExitStatus.SUCCESS.getExitCode();
  }

  protected Path getIdPath(RunMode mode) {
    return new Path(String.format("%s.%s.%s.%s",
        "/system/zonemigration.id",
        mode.toString().toLowerCase(),
        NetUtils.getLocalHostname(),
        Time.now()));
  }

  @Override
  protected boolean canSkip(String fullPath, HdfsLocatedFileStatus status) {
    if (super.canSkip(fullPath, status)) {
      return true;
    }

    LocatedBlock firstBlock = status.getLocatedBlocks().get(0);
    Map<String, Short> currentDistribution = ZoneUtil.getBlockDistribution(firstBlock);
    if (currentDistribution.isEmpty()) {
      LOG.error("There are no replicas for the missing block: {} by file: {}",
          firstBlock, fullPath);
      ZoneProgressTracker.incrFileCount();
      return true;
    }

    if (status.getErasureCodingPolicy() != null) {
      LOG.debug("Process data for ec file: {}", fullPath);
      if (!firstBlock.isStriped()) {
        LOG.debug("No need to process data for non ec file: {}", fullPath);
        ZoneProgressTracker.incrFileCount();
        return true;
      }
    }

    return false;
  }

  /**
   * Try to migrate the file from source DC to target DC according to rule.
   * This method may increase replication of this file if the target rule
   * needs more or less replicas. This method may migrate one replica to
   * target dc first to save bandwidth cross DC.
   */
  @Override
  protected void processFileWithPreMigration(String fullPath,
      HdfsLocatedFileStatus status, ReplicationRule rule, Mover.Result result) {
    LOG.debug("Processing file: {}, mode: {} from {} to {}", fullPath, this.runMode,
        sourceDC, targetDC);
    if (canSkip(fullPath, status)) {
      return;
    }

    // Process EC file
    if (status.getErasureCodingPolicy() != null) {
      processECFileDirectly(fullPath, status, result, sourceDC, targetDC);
      return;
    }

    final LocatedBlocks locatedBlocks = status.getLocatedBlocks();
    LocatedBlock firstBlock = locatedBlocks.get(0);
    Map<String, Short> currentDistribution = ZoneUtil.getBlockDistribution(firstBlock);

    ReplicationRule targetRule = rule;
    ReplicationRule currentRule = ReplicationRule.parseFromMap(currentDistribution);
    if (targetRule == null) {
      // If not set rule, it is generated from migrationRuleMap.
      targetRule = migrationRuleMap.getRuleFromDistribution(
          currentRule, status.getReplication(),
          sourceDC, targetDC, isDecrease);
    }

    // Means that this file can be skipped.
    if (targetRule == null) {
      LOG.debug("No need to process file: {} that are invalid rule by {}.",
          fullPath, this.runMode.getName());
      ZoneProgressTracker.incrFileCount();
      return;
    }

    if (targetRule.getReplica() != status.getReplication() && !allowChangeReplication) {
      LOG.warn("Ignore replica inconsistency for file: {} and appliedReplica: {}, " +
          "oldReplica :{}", fullPath, targetRule.getReplica(), status.getReplication());
      ZoneProgressTracker.incrFileCount();
      return;
    }

    LOG.info("Will apply the data rule from {} to {} for file: {} by {}.",
        currentRule, targetRule, fullPath, this.runMode.getName());

    // Starting to process this file.
    ZoneProgressTracker.queueFile(fullPath);
    boolean directlyMigrate = true;

    // If two replicas are required to migrate, the tool will migrate one replica first
    // then the rest replica can copy from the migrated replica directly.
    Set<String> dataCenters = currentRule.getDatacenters();
    if (targetRule.getReplica(targetDC) > 1
        && !dataCenters.contains(targetDC)
        && currentRule.getReplica() == status.getReplication()
        && currentRule.getReplica(sourceDC) > 1
        && enablePreMigration) {
      currentDistribution.put(sourceDC, (short) (currentDistribution.get(sourceDC) - 1));
      currentDistribution.put(targetDC, (short) 1);
      ReplicationRule preRule = ReplicationRule.parseFromMap(currentDistribution);
      LOG.info("Will pre-migrate 1 replica from {} to {} using preRule {} for {} by {}.",
          sourceDC, targetDC, preRule, fullPath, this.runMode.getName());
      try {
        // Migrate a replica first.
        processFileDirectly(fullPath, status, preRule, result);
        // PreMigrationChecker will migrate the remaining replicas.
        this.preMigrationChecker.preMigrateFile(new PreMigrationFile(fullPath, targetRule));
        directlyMigrate = false;
      } catch (Throwable e) {
        LOG.warn("Adding pre-migration file {} to the pre-migration queue is interrupted.",
            fullPath);
      }
    }

    if (directlyMigrate) {
      boolean processedByFetcher = false;
      try {
        LOG.debug("Processing file: {}, appliedRule: {}", fullPath, targetRule);
        processedByFetcher = processFileWithSetReplication(fullPath, status, targetRule, result);
      } finally {
        if (!processedByFetcher) {
          ZoneProgressTracker.dequeueFile(fullPath);
        }
      }
    }
  }

  @Override
  protected boolean processFileWithSetReplication(
      String fullPath, HdfsLocatedFileStatus status,
      ReplicationRule appliedRule, Mover.Result result) {
    LOG.debug("Processing file: {}, appliedRule: {}", fullPath, appliedRule);
    final LocatedBlocks locatedBlocks = status.getLocatedBlocks();
    // If the actual number of replicas is inconsistent with the rule's replicas,
    // first call setReplication.
    if (appliedRule.getReplica() != status.getReplication()) {
      if (allowChangeReplication) {
        try {
          long startRpcTime = Time.monotonicNow();
          LOG.debug("Before set replication: distribution is {}, appliedRule is {} for file: {}",
              ZoneUtil.getBlockDistribution(status.getLocatedBlocks().get(0)),
              appliedRule, fullPath);
          this.dfs.getClient().setReplication(fullPath, appliedRule.getReplica());
          ZoneProgressTracker.addSetReplicationTime((Time.monotonicNow() - startRpcTime));
          // Add this file to Coordinator and let Fetcher migrate the remaining replicas.
          this.coordinator.addFile(fullPath, appliedRule,
              appliedRule.getReplica() - status.getReplication(),
              locatedBlocks.getLocatedBlocks().size());
          return true;
        } catch (IOException e) {
          LOG.warn("Set replication fails for file: {}", fullPath, e);
          result.setRetryFailed();
        }
      } else {
        LOG.warn("Ignore replica not consistent for file: {}", fullPath);
      }
    } else {
      processFileDirectly(fullPath, status, appliedRule, result);
    }
    return false;
  }

  /**
   * Process EC file, move all the internal blocks from sourceDC to targetDC.
   */
  private void processECFileDirectly(String fullPath, HdfsLocatedFileStatus status,
      Mover.Result result, String sourceDC, String targetDC) {
    final ErasureCodingPolicy erasureCodingPolicy = status.getErasureCodingPolicy();

    ZoneProgressTracker.queueFile(fullPath);
    try {
      for (LocatedBlock block : status.getLocatedBlocks().getLocatedBlocks()) {
        List<ZoneMoveItem> moveItems = new ArrayList<>();
        Map<String, Short> distribution = ZoneUtil.getBlockDistribution(block);
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
    } finally {
      ZoneProgressTracker.dequeueFile(fullPath);
    }
  }

  class MigrationCoordinatorFetcher extends CoordinatorFetcher {
    private final long preMigrationCheckInterval;

    MigrationCoordinatorFetcher(Configuration conf) {
      super("ZoneMoverMigration-CoordinatorFetcher");
      this.preMigrationCheckInterval = conf.getLong(
          DFSConfigKeys.DFS_ZONE_MIGRATION_PRE_MIGRATION_CHECK_INTERVAL_KEY,
          DFSConfigKeys.DFS_ZONE_MIGRATION_PRE_MIGRATION_CHECK_INTERVAL_DEFAULT);
    }

    @Override
    public void run() {
      LOG.info("ZoneMoverMigration-CoordinatorFetcher is started.");
      long lastRecord = Time.monotonicNow();
      ZoneReplicationCoordinator.FileState fileState = null;
      while (true) {
        try {
          fileState = coordinator.getNextFinishedFile();
          String fullPath = fileState.getFilePath();
          HdfsLocatedFileStatus status = fileState.getFileStatus();
          if (!status.getLocatedBlocks().isLastBlockComplete()) {
            LOG.debug("Skip uncompleted file: {}", fullPath);
            continue;
          }
          processFileDirectly(fullPath, status, fileState.getRule(), result);
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
            ZoneProgressTracker.dequeueFile(fileState.getFilePath());
            fileState = null;
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
        + "\n\t[-changeReplica]\twhether to allow change the replication\""
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
        URI namenode = ZoneUtil.getNamespaceUri(commandLine, conf);
        List<Path> paths = ZoneMover.Cli.getPaths(commandLine);
        ReplicationRule rule = null;
        if (commandLine.hasOption("rule")) {
          rule = ZoneMover.Cli.getRule(commandLine);
        }
        String sourceDC = commandLine.getOptionValue("sourceDC");
        String targetDC = commandLine.getOptionValue("targetDC");
        boolean isDecrease = commandLine.hasOption("isDecrease");
        boolean changeReplica = commandLine.hasOption("changeReplica");

        // Default is batch mode.
        LOG.info("ZoneMigration starts with rule:{}, sourceDC: {}, targetDC: {}, " +
                "isDecrease: {}, changeReplica: {}", rule, sourceDC, targetDC,
            isDecrease, changeReplica);
        if (commandLine.hasOption("monitorByTrigger")) {
          return ZoneMigrationV2.runWithMonitorByTrigger(
              new ZoneMoverKafkaTrigger(conf, paths, namenode, true),
              conf, namenode, paths, rule, sourceDC, targetDC,
              isDecrease, changeReplica);
        } else {
          return ZoneMigrationV2.runWithBatch(conf, namenode, paths, rule,
              sourceDC, targetDC, isDecrease, changeReplica);
        }
      } catch (IOException e) {
        System.out.println(e + ".  Exiting ...");
        return ExitStatus.IO_EXCEPTION.getExitCode();
      } catch (ParseException | IllegalArgumentException e) {
        System.out.println(e + ".  Exiting ...");
        return ExitStatus.ILLEGAL_ARGUMENTS.getExitCode();
      } finally {
        LOG.info("ZoneMigration took {}", Time.monotonicNow() - startTime);
      }
    }
  }

  public static void main(String[] args) {
    if (DFSUtil.parseHelpArgument(args, ZoneMigrationV2.Cli.USAGE, System.out,
        true)) {
      System.exit(0);
    }

    try {
      System.exit(ToolRunner.run(new HdfsConfiguration(), new ZoneMigrationV2.Cli(), args));
    } catch (Throwable e) {
      LOG.error("Exiting ZoneMigration due to an exception", e);
      System.exit(-1);
    }
  }
}
