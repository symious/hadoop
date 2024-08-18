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
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.server.balancer.ExitStatus;
import org.apache.hadoop.hdfs.server.mover.Mover;
import org.apache.hadoop.hdfs.server.zoneservice.metrics.ZoneProgressTracker;
import org.apache.hadoop.hdfs.server.zoneservice.utils.RunMode;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.util.Time.now;

public class ZoneMoverWithDRV2 extends ZoneMoverV2 {

  public static final Logger LOG = LoggerFactory.getLogger(ZoneMoverWithDR.class);
  private final RunMode runMode;
  private final boolean useAccessTime;
  private final boolean skipReplica;
  private final boolean skipEC;
  private final boolean skipCheckCold;
  private final long drColdDataThresholdMS;
  private Set<String> drDataCenters;
  private Map<Short, ReplicationRule> drReplicationRuleForColdData;
  private Map<Short, ReplicationRule> drStripedBlockRule;

  public ZoneMoverWithDRV2(Configuration conf, URI nameNode, List<Path> paths,
      RunMode runMode, ZoneMoverTrigger zoneMoverTrigger, boolean useAccessTime,
      boolean skipReplica, boolean skipEC, boolean skipCheckCold)
      throws IOException {
    super(conf, nameNode, paths, runMode, zoneMoverTrigger);
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
  protected boolean canSkip(String fullPath, HdfsLocatedFileStatus status) {
    if  (super.canSkip(fullPath, status)) {
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

    if (!drDataCenters.containsAll(currentDistribution.keySet())) {
      LOG.error("Cannot generate valid replica rule for file: {}", fullPath);
      ZoneProgressTracker.incrFileCount();
      return true;
    }

    if (status.getErasureCodingPolicy() != null) {
      if (skipEC) {
        LOG.debug("No need to process data for ec file: {}.", fullPath);
        ZoneProgressTracker.incrFileCount();
        return true;
      }
    } else if (skipReplica) {
      LOG.debug("No need to process data for replication file: {}.", fullPath);
      ZoneProgressTracker.incrFileCount();
      return true;
    }

    return false;
  }

  /**
   * Migrate file for DR. This method generates target rule for EC/Contiguous
   * files according configuration and replicas.
   * This method may pre-migrate one replica to target DC for files.
   */
  @Override
  protected void processFileWithPreMigration(String fullPath, HdfsLocatedFileStatus status,
      ReplicationRule rule, Mover.Result result) {
    LOG.debug("Processing file: {}, mode: {}", fullPath, runMode);
    if (canSkip(fullPath, status)) {
      return;
    }

    if (status.getErasureCodingPolicy() != null) {
      processECFileDirectly(fullPath, status, result);
    } else { // Process contiguous file.
      short repl = status.getReplication();
      ReplicationRule targetRule = null;
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
        targetRule = drReplicationRuleForColdData.get(repl);
      }

      LocatedBlock firstBlock = status.getLocatedBlocks().get(0);
      Map<String, Short> currentDistribution = ZoneUtil.getBlockDistribution(firstBlock);
      if (targetRule == null) {
        HashMap<String, Integer> dcMap = new HashMap<>();
        for (String dc : drDataCenters) {
          dcMap.put(dc, Integer.valueOf(currentDistribution.getOrDefault(dc, (short) 0)));
        }
        targetRule = ReplicationRuleUtil.generateRuleForDR(dcMap, repl);
        if (targetRule == null) {
          LOG.debug("No need to process file: {} that are invalid rule by {}.",
              fullPath, runMode.getName());
          ZoneProgressTracker.incrFileCount();
          return;
        }
      }

      ReplicationRule oldRule = ReplicationRule.parseFromMap(currentDistribution);
      LOG.info("Will apply the data rule from {} to {} for file: {} by {}.", oldRule,
          targetRule, fullPath, runMode.getName());

      // Start migrate this file
      ZoneProgressTracker.queueFile(fullPath);
      boolean directlyMigrate = true;

      if (oldRule.getDatacenters().size() == 1) {
        String currentDC = oldRule.getDatacenters().iterator().next();
        // If two replicas are required to migrate, the tool will migrate one replica first
        // then the rest replica can copy from the migrated replica directly.
        if (repl - targetRule.getReplica(currentDC) > 1 && enablePreMigration) {
          // Generate the pre-migration rule to migrate one replica first to target DC.
          List<String> tmpDataCenters = new ArrayList<>(drDataCenters);
          tmpDataCenters.remove(currentDC);
          currentDistribution.put(currentDC, (short) (currentDistribution.get(currentDC) - 1));
          currentDistribution.put(tmpDataCenters.get(0), (short) 1);
          ReplicationRule preRule = ReplicationRule.parseFromMap(currentDistribution);

          LOG.info("Will pre-migrate 1 replica from {} to {} using preRule {} for {} by {}.",
              currentDC, tmpDataCenters.get(0), preRule, fullPath, runMode.getName());
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
      }
      if (directlyMigrate) {
        boolean processedByFetcher = false;
        try {
          processedByFetcher = processFileWithSetReplication(fullPath, status, targetRule, result);
        } finally {
          if (!processedByFetcher) {
            ZoneProgressTracker.dequeueFile(fullPath);
          }
        }
      }
    }
  }

  /**
   * This method may increase/decrease replication of the file.
   * Tips: DR will not change the replication.
   */
  @Override
  protected boolean processFileWithSetReplication(String fullPath,
      HdfsLocatedFileStatus status, ReplicationRule targetRule, Mover.Result result) {
    processFileDirectly(fullPath, status, targetRule, result);
    return false;
  }

  protected boolean isValidDistribution(Map<String, Short> currentDistribution) {
    return drDataCenters.containsAll(currentDistribution.keySet());
  }

  /**
   * Migrate EC file for DR.
   */
  private void processECFileDirectly(String fullPath,
      HdfsLocatedFileStatus status, Mover.Result result) {
    LocatedBlock locatedBlock = status.getLocatedBlocks().get(0);
    if (!locatedBlock.isStriped()) {
      LOG.debug("No need to process data for non ec file: {}", fullPath);
      ZoneProgressTracker.incrFileCount();
      return;
    }
    final ErasureCodingPolicy ecPolicy = status.getErasureCodingPolicy();
    int totalBlockNum = ecPolicy.getNumParityUnits() + ecPolicy.getNumDataUnits();
    ReplicationRule targetRule = drStripedBlockRule.get((short) totalBlockNum);
    if (targetRule == null) {
      LOG.info("No need to process data for ec file: {}, because codec name: {} is not set.",
          fullPath, ecPolicy.getName());
      ZoneProgressTracker.incrFileCount();
      return;
    }

    ZoneProgressTracker.queueFile(fullPath);
    try {
      for (LocatedBlock lb : status.getLocatedBlocks().getLocatedBlocks()) {
        // Retrieve rule based on the total number of blocks in the striped block.
        LocatedStripedBlock stripedBlock = (LocatedStripedBlock) locatedBlock;
        int blockNumExpected = Math.min(ecPolicy.getNumDataUnits(),
            (int) ((stripedBlock.getBlockSize() - 1) / ecPolicy.getCellSize() + 1)) +
            ecPolicy.getNumParityUnits();

        // If the block group is full blocks, return the rule directly.
        // Otherwise, need to be generated new rule.
        if (blockNumExpected < totalBlockNum) {
          targetRule = ReplicationRuleUtil.generateStripedBlockRuleForDR(
              targetRule, blockNumExpected);
        }
        processLocatedBlock(fullPath, lb, targetRule, result, ecPolicy);
      }
    } finally {
      ZoneProgressTracker.dequeueFile(fullPath);
    }
  }

  /**
   * Start ZoneMover to migrate code files for DR.
   */
  public static int runWithColdDataReplication(Configuration conf, URI nameNode,
      List<Path> paths, boolean useAccessTime, boolean skipReplica, boolean skipEC,
      boolean skipCheckCold) throws IOException, InterruptedException {

    ZoneProgressTracker.startCountingInitTime();
    LOG.info("Start to apply dr cold data rule to namenode: {}, path: {}, useAccessTime: {}, " +
            "skipReplica: {}, skipEC: {}, skipCheckCold: {}", nameNode, paths, useAccessTime,
        skipReplica, skipEC, skipCheckCold);
    if (paths.isEmpty()) {
      ZoneProgressTracker.finishCountingInitTimeAndLog();
      return ExitStatus.SUCCESS.getExitCode();
    }

    ZoneMoverWithDRV2 zm = null;
    try {
      zm = new ZoneMoverWithDRV2(conf, nameNode, paths, RunMode.COLD, null,
          useAccessTime, skipReplica, skipEC, skipCheckCold);
      zm.start();

      ZoneProgressTracker.finishCountingInitTimeAndLog();

      // Migration files
      return zm.migratePaths().getExitCode();
    } finally {
      ZoneProgressTracker.checkForLeak();
      if (zm != null) {
        zm.shutdown();
      }
    }
  }

  /**
   * Start ZoneMover to migrate new generated files.
   */
  public static int runWithNewDataReplication(ZoneMoverTrigger zoneMoverTrigger,
      Configuration conf, URI namenode, List<Path> paths, boolean skipReplica,
      boolean skipEC) throws IOException {
    if (paths.isEmpty()) {
      return ExitStatus.SUCCESS.getExitCode();
    }

    ZoneMoverWithDRV2 zm = null;
    try {
      zm = new ZoneMoverWithDRV2(conf, namenode, paths, RunMode.MONITOR,
          zoneMoverTrigger, false, skipReplica, skipEC, false);
      return zm.startInTriggerMonitor();
    } finally {
      if (zm != null) {
        zm.shutdown();
      }
      if (zoneMoverTrigger != null) {
        zoneMoverTrigger.shutdown();
      }
    }
  }

  protected Path getIdPath(RunMode mode) {
    return new Path(String.format("%s.%s.%s.%s",
        "/system/zonemoverwithdr.id",
        mode.toString().toLowerCase(),
        NetUtils.getLocalHostname(), Time.now()));
  }

  /**
   * Cli for ZoneMoverWithDR.
   */
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
        URI namenode = ZoneUtil.getNamespaceUri(commandLine, conf);
        boolean useAccessTime = commandLine.hasOption("-useAccessTime");
        boolean skipReplica = commandLine.hasOption("-skipReplica");
        boolean skipEC = commandLine.hasOption("-skipEC");
        boolean skipCheckCold = commandLine.hasOption("-skipCheckCold");
        List<Path> paths = ZoneMoverV2.Cli.getPaths(commandLine);
        if (commandLine.hasOption("cold")) {
          return ZoneMoverWithDRV2.runWithColdDataReplication(conf, namenode,
              paths, useAccessTime, skipReplica, skipEC, skipCheckCold);
        } else if (commandLine.hasOption("monitorByTrigger")) {
          ZoneMoverTrigger zoneMoverTrigger =
              new ZoneMoverKafkaTrigger(conf, paths, namenode, true);
          return ZoneMoverWithDRV2.runWithNewDataReplication(zoneMoverTrigger,
              conf, namenode, paths, skipReplica, skipEC);
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
        LOG.info("ZoneMoverWithDR took {}.",
            StringUtils.formatTime(Time.monotonicNow() - startTime));
      }
      return 0;
    }
  }

  public static void main(String[] args) {
    if (DFSUtil.parseHelpArgument(args, ZoneMoverWithDRV2.Cli.USAGE,
        System.out, true)) {
      System.exit(0);
    }

    try {
      System.exit(
          ToolRunner.run(new HdfsConfiguration(), new ZoneMoverWithDRV2.Cli(), args));
    } catch (Throwable e) {
      LOG.error("Exiting ZoneMoverWithDR due to an exception", e);
      System.exit(-1);
    }
  }
}
