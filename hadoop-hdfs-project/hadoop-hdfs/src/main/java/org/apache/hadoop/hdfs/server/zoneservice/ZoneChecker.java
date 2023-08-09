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
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher;
import org.apache.hadoop.hdfs.server.balancer.ExitStatus;
import org.apache.hadoop.hdfs.server.balancer.NameNodeConnector;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ZoneChecker {
  private static final Logger LOG = LoggerFactory.getLogger(ZoneChecker.class);
  private static final String ROOT = "/";
  private final DFSClient dfs;
  private float ratio;
  private static final int MIN_FILE_NUM = 1;
  private static final String BLOCK_SUMMARY_FORMAT = "DC:%-15sBlocks Number:%-20d" +
      "Data Size:%-20d";
  private static final String SUMMARY_FORMAT = "Distribution:%-30sBlocks Number:%9d (%5.2f%%)    Total Size:%d";

  public ZoneChecker(NameNodeConnector nnc, Configuration conf) {
    Dispatcher dispatcher = new ZoneDispatcher(
        nnc, Collections.emptySet(),
        Collections.emptySet(), 0, 0,
        0, conf, 0, 0);
    ratio = conf.getFloat(DFSConfigKeys.DFS_ZONECHECKER_DEFAULT_RATIO,
        DFSConfigKeys.DFS_ZONECHECKER_DEFAULT_RATIO_DEFAULT);
    this.dfs = dispatcher.getDistributedFileSystem().getClient();
  }

  private static int run(Configuration conf, URI nameNode,
      String path, Float ratio, boolean blockSummary) {
    return run(conf, nameNode, path, ratio, blockSummary, false);
  }

  /**
   * Run with prepared arguments.
   * @param conf configuration
   * @param path the path to be checked
   * @param blockSummary   flag to check replica and storage size under every DataCenter
   * @param countOnly      flag to count the block number and size for every distribution
   */
  private static int run(Configuration conf, URI nameNode,
      String path, Float ratio, boolean blockSummary, boolean countOnly) {
    LOG.info("Start to check path: " + path);
    NameNodeConnector nnc;
    try {
      nnc = new NameNodeConnector(nameNode,
          Collections.singletonList(new Path(path)),
          conf, 1);
      final ZoneChecker zch = new ZoneChecker(nnc, conf);
      //if ratio is inputted by user, set it
      if (blockSummary) {
        LOG.info("Start to summary the blocks of {}", path);
      } else if (countOnly) {
        LOG.info("Start to count the block number only for {}", path);
      } else {
        if (ratio <= 0.0f) {
          LOG.info("Start to check path: " + path + " with default ratio.");
        } else {
          LOG.info("Start to check path: " + path + " with ratio " + ratio);
          zch.setRatio(ratio);
        }
      }
      Map<ReplicationRule, Set<String>> rulePathMap = new HashMap<>();
      Map<String, List<Long>> dcStatMap = new HashMap<>();
      zch.getReplicaInfo(path, rulePathMap, dcStatMap, blockSummary, countOnly);
      if (blockSummary) {
        printBlockSummary(dcStatMap);
      } else if (countOnly) {
        printFileCount(dcStatMap);
      } else {
        printResult(rulePathMap);
      }
      return 0;
    } catch (IOException e) {
      LOG.warn("ZoneChecker failed check {} from {}", path, nameNode, e);
      return 1;
    }
  }

  public static Map<ReplicationRule, Set<String>> getReplicaRule(
      Configuration conf, URI namenode, String path, Float ratio) {
    LOG.info("Start to check path: " + path + " with ratio " + ratio);
    NameNodeConnector nnc;
    try {
      nnc = new NameNodeConnector(namenode,
          Collections.singletonList(new Path(path)),
          conf, 1);
      final ZoneChecker zch = new ZoneChecker(nnc, conf);
      //if ratio is inputted by user, set it
      if (ratio > 0.0f) {
        zch.setRatio(ratio);
      }
      Map<ReplicationRule, Set<String>> rulePathMap = new HashMap<>();
      zch.getReplicaInfo(path, rulePathMap, new HashMap<String, List<Long>>(), false, false);
      return rulePathMap;
    } catch (IOException e) {
      LOG.error("ZoneChecker meets the IOException: ", e);
      return null;
    }
  }

  public static Map<String, List<Long>> getBlockSummary(
      Configuration conf, URI namenode, String path) {
    LOG.info("Start to get block summary of path: " + path);
    NameNodeConnector nnc;
    try {
      // Clear up the map
      Map<String, List<Long>> dcBlockStat = new HashMap<>();
      nnc = new NameNodeConnector(namenode,
          Collections.singletonList(new Path(path)), conf, 1);
      final ZoneChecker zch = new ZoneChecker(nnc, conf);
      zch.getReplicaInfo(path, new HashMap<ReplicationRule, Set<String>>(), dcBlockStat, true, false);
      return dcBlockStat;
    } catch (IOException e) {
      LOG.error("ZoneChecker meets the IOException: ", e);
      return null;
    }
  }

  public static Map<String, List<Long>> getCountSummary(
      Configuration conf, URI namenode, String path) {
    LOG.info("Start to get block summary of path: " + path);
    NameNodeConnector nnc;
    try {
      // Clear up the map
      Map<String, List<Long>> dcBlockStat = new HashMap<>();
      nnc = new NameNodeConnector(namenode,
          Collections.singletonList(new Path(path)), conf, 1);
      final ZoneChecker zch = new ZoneChecker(nnc, conf);
      zch.getReplicaInfo(path, new HashMap<ReplicationRule, Set<String>>(), dcBlockStat, false, true);
      return dcBlockStat;
    } catch (IOException e) {
      LOG.error("ZoneChecker meets the IOException: ", e);
      return null;
    }
  }

  static class Cli extends Configured implements Tool {
    private static final String USAGE = "Usage: hdfs zonechecker"
        + "\n\t[-namespace <namespace>]\tthe namespace to be checked."
        + "\n\t-path <path>\tthe path to be checked."
        + "\n\t[-ratio <ratio>]\tif the path is a directory, the ratio of "
        + "files will be checked"
        + "\n\t[-blockSummary]\tCheck data size and blocks number of DCs"
        + "\n\t[-count]\tCount the number of blocks under the every distribution";

    private static Options buildCliOptions() {
      Options options = new Options();
      Option option = new Option(
          null, "namespace", true,
          "the namespace to be checked");
      options.addOption(option);

      option = new Option(null, "path", true,
          "the path to be checked");
      option.setRequired(true);
      options.addOption(option);

      option = new Option(
          null, "ratio", true,
          "the ratio of files will be checked");
      options.addOption(option);

      option = new Option(
          null, "blockSummary", false,
          "check data size and blocks number of DCs");
      options.addOption(option);

      option = new Option(
          null, "count", false,
          "Count the number of block under the every distribution");
      options.addOption(option);
      return options;
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
     * Get the path from args
     */
    private static String getPath(CommandLine line)
        throws IllegalArgumentException {
      String path = line.getOptionValue("path");
      if (!path.startsWith(ROOT)) {
        throw new IllegalArgumentException("Please provide a valid path!");
      }
      return path;
    }

    /**
     * Get the ratio from args
     */
    private static Float getRatio(CommandLine line)
        throws IllegalArgumentException {
      float ratio;
      if (line.hasOption("ratio")) {
        ratio = Float.parseFloat(line.getOptionValue("ratio"));
      } else {
        return -1.0f;
      }
      if (ratio > 1.0f || ratio < 0.0f) {
        throw new IllegalArgumentException("Please provide a valid ratio" +
            " which need to be in [0,1]!");
      }
      return ratio;
    }

    /**
     * Get block summary from args
     */
    private static boolean getBlockSummary(CommandLine line) {
      return line.hasOption("blockSummary");
    }

    /**
     * Get block count only under every distribution
     */
    private static boolean getCountOnly(CommandLine line) {
      return line.hasOption("count");
    }

    @Override
    public int run(String[] args) {
      long startTime = Time.monotonicNow();
      final Configuration conf = getConf();
      final Options options = buildCliOptions();
      CommandLineParser parser = new GnuParser();
      CommandLine commandLine;
      try {
        commandLine = parser.parse(options, args, true);
        if (getCountOnly(commandLine) && getBlockSummary(commandLine)) {
          throw new IllegalArgumentException(
              "-blockSummary & -count cannot be used at the same time");
        }
        return run(conf, getNamespaceUri(commandLine, conf),
            getPath(commandLine), getRatio(commandLine), getBlockSummary(commandLine),
            getCountOnly(commandLine));
      } catch (ParseException | IllegalArgumentException e) {
        System.out.println(e + ".  Exiting ...");
        return ExitStatus.ILLEGAL_ARGUMENTS.getExitCode();
      } finally {
        System.out.format("ZoneChecker took "
            + StringUtils.formatTime(Time.monotonicNow() - startTime) + '\n');
      }
    }

    /**
     * Run with given ratio.
     */
    int run(Configuration conf, URI namenodeURI, String path, Float ratio, boolean blockSummary,
        boolean countOnly) {
      return ZoneChecker.run(conf, namenodeURI, path, ratio, blockSummary, countOnly);
    }
  }

  public void getReplicaInfo(
      String fullPath, Map<ReplicationRule, Set<String>> rulePathMap,
      Map<String, List<Long>> dcBlockStat, boolean blockSummaryFlag, boolean countOnly) {
    for (byte[] lastReturnedName = HdfsFileStatus.EMPTY_NAME;;) {
      final DirectoryListing children;
      try {
        children = dfs.listPaths(fullPath, lastReturnedName, true);
      } catch(IOException e) {
        LOG.warn("Failed to list directory " + fullPath
            + ". Ignore the directory and continue.", e);
        return;
      }
      if (children == null) {
        return;
      }
      HdfsFileStatus[] partialList = children.getPartialListing();
      int threshold = Math.max(MIN_FILE_NUM,
          Math.round(partialList.length * ratio));
      for (HdfsFileStatus child : getRandomList(partialList, threshold)) {
        // To make sure when the sub-dir is merged in rulePathMap, sub result is fully merged
        Map<ReplicationRule, Set<String>> subRulePathMap = new HashMap<>();
        getReplicaInfoRecursively(fullPath, child, subRulePathMap, dcBlockStat, blockSummaryFlag, countOnly);
        if (!blockSummaryFlag && !countOnly) {
          mergeRules(rulePathMap, subRulePathMap);
        }
      }

      if (!blockSummaryFlag && rulePathMap.keySet().size() == 1 && !countOnly) {
        rulePathMap.put(rulePathMap.keySet().iterator().next(),
            new HashSet<>(Collections.singletonList(fullPath)));
      }

      if (children.hasMore()) {
        lastReturnedName = children.getLastName();
      } else {
        break;
      }
    }
  }

  /** @return whether the check requires next round */
  private void getReplicaInfoRecursively(
      String parent, HdfsFileStatus status, Map<ReplicationRule, Set<String>> rulePathMap,
      Map<String, List<Long>> dcBlockStat, boolean blockSummaryFlag, boolean countOnly) {
    String fullPath = status.getFullName(parent);
    if (status.isDir()) {
      if (!fullPath.endsWith(Path.SEPARATOR)) {
        fullPath = fullPath + Path.SEPARATOR;
      }

      getReplicaInfo(fullPath, rulePathMap, dcBlockStat, blockSummaryFlag, countOnly);
    } else if (!status.isSymlink()) { // file
      try {
        HdfsLocatedFileStatus locStatus = (HdfsLocatedFileStatus) status;
        final LocatedBlocks locatedBlocks = locStatus.getLocatedBlocks();
        final boolean lastBlkComplete = locatedBlocks.isLastBlockComplete();
        List<LocatedBlock> lbs = locatedBlocks.getLocatedBlocks();
        for (int i = 0; i < lbs.size(); i++) {
          if (i == lbs.size() - 1 && !lastBlkComplete) {
            // last block is incomplete, skip it
            continue;
          }
          LocatedBlock lb = lbs.get(i);
          Map<String, Short> mapDCReplica = ZoneMover.getBlockDistribution(lb);
          if (lb.getLocations().length != status.getReplication()
              || lb.getLocations().length > 5) {
            LOG.warn("Found the abnormal file {} with replication {} and distribution {}",
                fullPath, status.getReplication(), ReplicationRule.parseFromMap(mapDCReplica));
          }
          if (blockSummaryFlag) {
            for (String dc : mapDCReplica.keySet()) {
              if (dcBlockStat.containsKey(dc)) {
                dcBlockStat.get(dc).set(0, dcBlockStat.get(dc).get(0) + mapDCReplica.get(dc));
                dcBlockStat.get(dc).set(1, dcBlockStat.get(dc).get(1) +
                    lb.getBlockSize() * mapDCReplica.get(dc));
              } else {
                dcBlockStat.put(dc,
                    Arrays.asList(new Long(mapDCReplica.get(dc)),
                        lb.getBlockSize() * mapDCReplica.get(dc)));
              }
            }
          } else if (countOnly) {
            String replicationRule =
                ReplicationRule.parseFromMap(mapDCReplica).toString();
            if (dcBlockStat.containsKey(replicationRule)) {
              dcBlockStat.get(replicationRule).set(0, dcBlockStat.get(replicationRule).get(0) + 1);
              dcBlockStat.get(replicationRule).set(1, dcBlockStat.get(replicationRule).get(1) +
                  lb.getBlockSize());
            } else {
              dcBlockStat.put(replicationRule, Arrays.asList(1L, lb.getBlockSize()));
            }
          } else {
            ReplicationRule replicationRule =
                ReplicationRule.parseFromMap(mapDCReplica);
            if (rulePathMap.containsKey(replicationRule)) {
              rulePathMap.get(replicationRule).add(fullPath);
            } else {
              rulePathMap.put(replicationRule,
                  new HashSet<>(Collections.singletonList(fullPath)));
            }
          }
        }

      } catch (Exception e) {
        LOG.warn("Failed to check the status of " + parent + ". Ignore it and continue.", e);
      }
    }
  }

  /**
   * Choose random list
   */
  private List<HdfsFileStatus> getRandomList(
      HdfsFileStatus[] hdfsFileStatuses, int threshold) {
    List<HdfsFileStatus> fileStatusList = Arrays.asList(hdfsFileStatuses);
    if (fileStatusList.isEmpty()) { return fileStatusList; }
    Collections.shuffle(fileStatusList);
    return fileStatusList.subList(0, threshold);
  }

  private void mergeRules(
      Map<ReplicationRule, Set<String>> rule1,
      Map<ReplicationRule, Set<String>> rule2) {
    for (Map.Entry<ReplicationRule, Set<String>> entry:rule2.entrySet()) {
      ReplicationRule key = entry.getKey();
      List<ReplicationRule> keyList = new ArrayList<>(rule1.keySet());
      if (keyList.contains(key)) {
        rule1.get(key).addAll(entry.getValue());
      } else {
        rule1.put(key, entry.getValue());
      }
    }
  }

  private static void printBlockSummary(Map<String, List<Long>> dcBlockStat) {
    System.out.println("Block summary:");
    for (String dc: dcBlockStat.keySet()) {
      System.out.printf(
          (BLOCK_SUMMARY_FORMAT) + "%n", dc, dcBlockStat.get(dc).get(0), dcBlockStat.get(dc).get(1));
    }
  }

  private static void printFileCount(Map<String, List<Long>> dcBlockStat) {
    System.out.println("Summary:");
    long totalBlocks = 0;
    for (String distribution: dcBlockStat.keySet()) {
      totalBlocks += dcBlockStat.get(distribution).get(0);
    }
    for (String distribution: dcBlockStat.keySet()) {
      System.out.printf((SUMMARY_FORMAT) + "%n", distribution, dcBlockStat.get(distribution).get(0),
          (double) dcBlockStat.get(distribution).get(0) / totalBlocks * 100,
          dcBlockStat.get(distribution).get(1));
    }
  }

  private static void printResult(Map<ReplicationRule, Set<String>> map) {
    System.out.println("Zone checker result: ");
    for (Map.Entry<ReplicationRule, Set<String>> entry: map.entrySet()) {
      ReplicationRule replicationRule = entry.getKey();
      System.out.println(replicationRule.toString() + ":");
      for (String path: entry.getValue()) {
        System.out.println("  " + path);
      }
    }
  }

  private void setRatio(Float ratio) {
    this.ratio = ratio;
  }

  /**
   * Run a Checker in command line.
   *
   * @param args Command line arguments
   */
  public static void main(String[] args) {
    if (DFSUtil.parseHelpArgument(args, ZoneChecker.Cli.USAGE,
        System.out, true)) {
      System.exit(0);
    }

    try {
      System.exit(ToolRunner.run(new HdfsConfiguration(),
          new ZoneChecker.Cli(), args));
    } catch (Throwable e) {
      LOG.error("Exiting " + ZoneChecker.class.getSimpleName()
          + " due to an exception", e);
      System.exit(-1);
    }
  }
}