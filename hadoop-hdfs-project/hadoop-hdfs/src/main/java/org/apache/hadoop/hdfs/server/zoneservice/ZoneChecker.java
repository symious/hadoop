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

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.net.DFSNetworkTopologyWithDataCenter;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.balancer.ExitStatus;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.concurrent.HadoopThreadPoolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class ZoneChecker {
  private static final Logger LOG = LoggerFactory.getLogger(ZoneChecker.class);
  private static final String ROOT = "/";
  private final DistributedFileSystem dfs;
  private float ratio;
  private static final int MIN_FILE_NUM = 1;
  private static final String BLOCK_SUMMARY_FORMAT = "DC:%-15sBlocks Number:%-20d" +
      "Data Size:%-20d";
  private static final String SUMMARY_FORMAT = "Distribution:%-30sBlocks Number:%9d (%5.2f%%)    Total Size:%d";

  private static HadoopThreadPoolExecutor EXECUTOR = null;
  private static int EXECUTOR_REFERENCE = 0;

  public ZoneChecker(DistributedFileSystem dfs, Configuration conf) {
    ratio = conf.getFloat(DFSConfigKeys.DFS_ZONECHECKER_DEFAULT_RATIO,
        DFSConfigKeys.DFS_ZONECHECKER_DEFAULT_RATIO_DEFAULT);
    this.dfs = dfs;
  }

  public ZoneChecker(Configuration conf) throws IOException {
    ratio = conf.getFloat(DFSConfigKeys.DFS_ZONECHECKER_DEFAULT_RATIO,
        DFSConfigKeys.DFS_ZONECHECKER_DEFAULT_RATIO_DEFAULT);
    this.dfs = (DistributedFileSystem) FileSystem.get(conf);
  }

  private synchronized static HadoopThreadPoolExecutor createExecutor(int numThreads) {
     if (EXECUTOR == null) {
       ThreadFactory tf = new ThreadFactoryBuilder()
           .setNameFormat("ZoneChecker Executor #%d")
           .build();
       EXECUTOR = new HadoopThreadPoolExecutor(numThreads, numThreads,
           0L, TimeUnit.MILLISECONDS,
           new LinkedBlockingQueue<>(), tf);
     } else {
       if (numThreads != EXECUTOR.getCorePoolSize()) {
         EXECUTOR.setCorePoolSize(numThreads);
         EXECUTOR.setMaximumPoolSize(numThreads);
       }
     }
     EXECUTOR_REFERENCE += 1;
     return EXECUTOR;
  }

  private synchronized static void closeExecutor() {
    assert EXECUTOR != null;
    EXECUTOR_REFERENCE -= 1;
    if (EXECUTOR_REFERENCE == 0) {
      EXECUTOR.shutdown();
      EXECUTOR.shutdownNow();
      EXECUTOR = null;
    }
  }

  /**
   * Get block distribution for paths.
   * @param paths input path
   * @param threads the number of threads to check blocks distribution.
   *                These paths will be checked one by one if threads < 1
   * @return a mapping from path to ReplicationRule.
   */
  public Map<String, Set<ReplicationRule>> getBlockDistribution(
      List<String> paths, int threads) throws Exception {
    Map<String, Set<ReplicationRule>> blockDistribution = new ConcurrentHashMap<>();
    if (threads > 1) {
      ExecutorService executor = createExecutor(threads);
      try {
        List<Future<?>> futures = new ArrayList<>();
        for (String path : paths) {
          futures.add(executor.submit(() ->
              collectBlockDistribution(path, blockDistribution)));
        }

        for (Future<?> f : futures) {
          f.get();
        }
      } finally {
        closeExecutor();
      }
    } else {
      for (String path : paths) {
        collectBlockDistribution(path, blockDistribution);
      }
    }
    return blockDistribution;
  }

  /**
   * Getting validate DC names from Server.
   * Notices: Please don't call this method frequently, since it's heavy.
   */
  public Set<String> getValidateDCs() throws IOException {
    Set<String> dcs = new HashSet<>();
    DatanodeInfo[] datanodeInfos = this.dfs.getDataNodeStats(
        HdfsConstants.DatanodeReportType.LIVE);
    for (DatanodeInfo dn : datanodeInfos) {
      String dnDC = DFSNetworkTopologyWithDataCenter.getDataCenter(dn.getNetworkLocation());
      dcs.add(dnDC);
    }
    return dcs;
  }

  /**
   * Collection block distribution for the given path.
   */
  private void collectBlockDistribution(String path,
      Map<String, Set<ReplicationRule>> blockDistribution) {
    try {
      HdfsLocatedFileStatus fileStatus = this.dfs.getClient().getLocatedFileInfo(
          new Path(path).toUri().getPath(), false);
      final LocatedBlocks locatedBlocks = fileStatus.getLocatedBlocks();
      final boolean lastBlkComplete = locatedBlocks.isLastBlockComplete();
      List<LocatedBlock> lbs = locatedBlocks.getLocatedBlocks();
      blockDistribution.put(path, new HashSet<>());
      for (int i = 0; i < lbs.size(); i++) {
        if (i == lbs.size() - 1 && !lastBlkComplete) {
          // last block is incomplete, skip it
          continue;
        }
        LocatedBlock lb = lbs.get(i);
        Map<String, Short> mapDCReplica = ZoneMover.getBlockDistribution(lb);
        ReplicationRule replicationRule = ReplicationRule.parseFromMap(mapDCReplica);
        blockDistribution.get(path).add(replicationRule);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Run with prepared arguments.
   * @param conf configuration
   * @param path the path to be checked
   * @param blockSummary   flag to check replica and storage size under every DataCenter
   * @param countOnly      flag to count the block number and size for every distribution
   * @param countDepth     the depth at which block distribution is printed
   * @param threadCount    the count of threads to check the replication distribution
   */
  private static int check(Configuration conf, URI nameNode, String path, Float ratio,
      boolean blockSummary, boolean countOnly, int countDepth, int threadCount) {
    try {
      DistributedFileSystem fs = (DistributedFileSystem) FileSystem.get(nameNode, conf);
      final ZoneChecker zch = new ZoneChecker(fs, conf);
      //if ratio is inputted by user, set it
      if (blockSummary) {
        LOG.info("Start to summary the blocks of {}", path);
      } else if (countOnly) {
        LOG.info("Start to count the block number for {} at depth {}", path, countDepth);
      } else {
        if (ratio <= 0.0f) {
          LOG.info("Start to check path: {} with default ratio.", path);
        } else {
          LOG.info("Start to check path: {} with ratio {}", path, ratio);
          zch.setRatio(ratio);
        }
      }
      ConcurrentHashMap<ReplicationRule, Set<String>> rulePathMap = new ConcurrentHashMap<>();
      Map<String, List<Long>> dcStatMap = new HashMap<>();
      ZoneCheckerCountTree zcct = new ZoneCheckerCountTree(path, countDepth);
      if (threadCount > 1) {
        zch.concurrentlyCheck(path, rulePathMap, dcStatMap, zcct, blockSummary, countOnly, threadCount);
      } else {
        zch.check(path, rulePathMap, dcStatMap, zcct, blockSummary, countOnly);
      }
      if (blockSummary) {
        printBlockSummary(dcStatMap);
      } else if (countOnly) {
        printFileCount(zcct);
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
      Configuration conf, URI namenode, String path, float ratio, int threads) {
    LOG.info("Start to get replication rules for {} with ratio {} in {} threads.",
        path, ratio, threads);
    try {
      DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.get(namenode, conf);
      final ZoneChecker zch = new ZoneChecker(dfs, conf);
      //if ratio is inputted by user, set it
      if (ratio > 0.0f) {
        zch.setRatio(ratio);
      }
      ConcurrentHashMap<ReplicationRule, Set<String>> rulePathMap = new ConcurrentHashMap<>();
      if (threads > 1) {
        zch.concurrentlyCheck(path, rulePathMap, new HashMap<>(), null, false, false, threads);
      } else {
        zch.check(path, rulePathMap, new HashMap<>(), null, false, false);
      }
      return rulePathMap;
    } catch (IOException e) {
      LOG.error("ZoneChecker meets the IOException: ", e);
      return null;
    }
  }

  public static Map<String, List<Long>> getBlockSummary(
      Configuration conf, URI namenode, String path, int threads) {
    LOG.info("Start to get block summary of path {} in {} threads.", path, threads);
    try {
      // Clear up the map
      Map<String, List<Long>> dcBlockStat = new HashMap<>();
      DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.get(namenode, conf);
      final ZoneChecker zch = new ZoneChecker(dfs, conf);
      if (threads > 1) {
        zch.concurrentlyCheck(path, new ConcurrentHashMap<>(), dcBlockStat, null, true, false, threads);
      } else {
        zch.check(path, new HashMap<>(), dcBlockStat, null, true, false);
      }
      return dcBlockStat;
    } catch (IOException e) {
      LOG.error("ZoneChecker meets the IOException: ", e);
      return null;
    }
  }

  public static Map<String, List<Long>> getCountSummary(
      Configuration conf, URI namenode, String path, int threads) {
    LOG.info("Start to get block summary of path {} in {} threads.", path, threads);
    try {
      // Clear up the map
      Map<String, List<Long>> dcBlockStat = new HashMap<>();
      DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.get(namenode, conf);
      final ZoneChecker zch = new ZoneChecker(dfs, conf);
      ZoneCheckerCountTree zcct = new ZoneCheckerCountTree(path, 0);
      if (threads > 1) {
        zch.concurrentlyCheck(path, new ConcurrentHashMap<>(), dcBlockStat, zcct, false, true, threads);
      } else {
        zch.check(path, new HashMap<>(), dcBlockStat, zcct, false, true);
      }
      return zcct.getMap();
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
        + "\n\t[-count]\tCount the number of blocks under the every distribution"
        + "\n\t[-depth depth]\tthe depth at which block distribution is printed"
        + "\n\t[-threadCount threadCount]\tcheck replication distribution with multiple threads";

    private Options buildCliOptions() {
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

      option = new Option(
          null, "depth", true,
          "the depth at which block distribution is printed");
      options.addOption(option);

      option = new Option(
          null, "threadCount", true,
          "the number of threads to check");
      options.addOption(option);
      return options;
    }

    /**
     * Get the path from args
     */
    private String getPath(CommandLine line) throws IllegalArgumentException {
      String path = line.getOptionValue("path");
      if (!path.startsWith(ROOT)) {
        throw new IllegalArgumentException("Please provide a valid path!");
      }
      return path;
    }

    /**
     * Get the ratio from args
     */
    private Float getRatio(CommandLine line) throws IllegalArgumentException {
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
    private boolean getBlockSummary(CommandLine line) {
      return line.hasOption("blockSummary");
    }

    /**
     * Get block count only under every distribution
     */
    private boolean getCountOnly(CommandLine line) {
      return line.hasOption("count");
    }


    private int getCountDepth(CommandLine commandLine) {
      if (!commandLine.hasOption("depth")) {
        return 0;
      }
      if (!commandLine.hasOption("count")) {
        System.out.println("-depth option doesn't work without -count option");
        return 0;
      } else {
        return Integer.parseInt(commandLine.getOptionValue("depth"));
      }
    }

    private int getThreadCount(CommandLine commandLine) {
      if (!commandLine.hasOption("threadCount")) {
        return 1;
      } else {
        return Integer.parseInt(commandLine.getOptionValue("threadCount"));
      }
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
        return ZoneChecker.check(conf, ZoneMover.getNamespaceUri(commandLine, conf),
            getPath(commandLine), getRatio(commandLine), getBlockSummary(commandLine),
            getCountOnly(commandLine), getCountDepth(commandLine), getThreadCount(commandLine));
      } catch (ParseException | IllegalArgumentException e) {
        System.out.println(e + ".  Exiting ...");
        return ExitStatus.ILLEGAL_ARGUMENTS.getExitCode();
      } finally {
        System.out.format("ZoneChecker took "
            + StringUtils.formatTime(Time.monotonicNow() - startTime) + '\n');
      }
    }
  }

  /**
   * Checking the replication distribution with multiple threads.
   */
  void concurrentlyCheck(String fullPath, ConcurrentHashMap<ReplicationRule, Set<String>> rulePathMap,
      Map<String, List<Long>> dcBlockStat, ZoneCheckerCountTree zcct,
      boolean blockSummaryFlag, boolean countOnly, int threads) {
    LOG.info("Checking replication distribution of {} with {} thread(s).", fullPath, threads);
    long start = Time.monotonicNow();
    ForkJoinPool p = new ForkJoinPool(threads);
    CheckTask task = new CheckTask(this, dfs, fullPath, rulePathMap, dcBlockStat, zcct,
        blockSummaryFlag, countOnly, ratio);
    p.execute(task);
    task.join();
    p.shutdown();
    LOG.info("Replication distribution of {} completed in {}(ms).", fullPath,
        (Time.monotonicNow() - start));
  }

  /**
   * Checking the replication distribution with one thread.
   */
  void check(String fullPath, Map<ReplicationRule, Set<String>> rulePathMap,
      Map<String, List<Long>> dcBlockStat, ZoneCheckerCountTree zcct, boolean blockSummaryFlag,
      boolean countOnly) {
    for (byte[] lastReturnedName = HdfsFileStatus.EMPTY_NAME; ; ) {
      final DirectoryListing children;
      try {
        children = dfs.getClient().listPaths(fullPath, lastReturnedName, true);
      } catch (IOException e) {
        LOG.warn("Failed to list directory {}. Ignore the directory and continue.", fullPath, e);
        return;
      }
      if (children == null) {
        return;
      }
      HdfsFileStatus[] partialList = children.getPartialListing();
      int threshold = Math.max(MIN_FILE_NUM, Math.round(partialList.length * ratio));
      for (HdfsFileStatus child : getRandomList(partialList, threshold)) {
        // To make sure when the sub-dir is merged in rulePathMap, sub result is fully merged
        Map<ReplicationRule, Set<String>> subRulePathMap = new HashMap<>();
        getReplicaInfoRecursively(fullPath, child, subRulePathMap, dcBlockStat, zcct,
            blockSummaryFlag, countOnly);
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

  /**
   * @param dcBlockStat stores block states of DCs, the key is DC, the value of a DC is a list,
   *                    the first element of the list is the number of blocks,
   *                    the second element of the list is total size of blocks.
   */
  private void getReplicaInfoRecursively(String parent, HdfsFileStatus status,
      Map<ReplicationRule, Set<String>> rulePathMap, Map<String, List<Long>> dcBlockStat,
      ZoneCheckerCountTree zcct, boolean blockSummaryFlag, boolean countOnly) {
    String fullPath = status.getFullName(parent);
    if (status.isDir()) {
      if (!fullPath.endsWith(Path.SEPARATOR)) {
        fullPath = fullPath + Path.SEPARATOR;
      }
      check(fullPath, rulePathMap, dcBlockStat, zcct, blockSummaryFlag, countOnly);
    } else {
      getReplicaInfoOfFile(parent, (HdfsLocatedFileStatus) status, rulePathMap,
          dcBlockStat, zcct, blockSummaryFlag, countOnly);
    }
  }

  /**
   * Choose random list
   */
  private static List<HdfsFileStatus> getRandomList(HdfsFileStatus[] hdfsFileStatuses, int threshold) {
    List<HdfsFileStatus> fileStatusList = Arrays.asList(hdfsFileStatuses);
    if (fileStatusList.isEmpty()) {
      return fileStatusList;
    }

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

  @VisibleForTesting
  static void printFileCount(ZoneCheckerCountTree zcct) {
    System.out.println("Summary:");
    long totalBlocks = 0;
    for (long blocks : zcct.getRoot().getBlockCounts().values()) {
      totalBlocks += blocks;
    }
    recursivelyPrintFileCount(zcct.getRoot(), "", totalBlocks);
  }

  /**
   * Traverses through the {@link ZoneCheckerCountTree} in BFS, prints all the stuff
   */
  private static void recursivelyPrintFileCount(ZoneCheckerCountTreeNode node, String pathPrefix,
      long totalBlocks) {
    if (pathPrefix == null || pathPrefix.isEmpty()) {
      pathPrefix = node.name;
    } else {
      pathPrefix += Path.SEPARATOR_CHAR + node.name;
    }
    System.out.printf("Path: %s%n", pathPrefix);
    for (String distribution: node.getBlockCounts().keySet()) {
      System.out.printf((SUMMARY_FORMAT) + "%n", distribution, node.getBlockCounts().get(distribution),
          (double) node.getBlockCounts().get(distribution) / totalBlocks * 100,
          node.getByteCounts().get(distribution));
    }
    for (ZoneCheckerCountTreeNode child: node.children.values()) {
      recursivelyPrintFileCount(child, pathPrefix, totalBlocks);
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
      LOG.error("Exiting {} due to an exception", ZoneChecker.class.getSimpleName(), e);
      System.exit(-1);
    }
  }

  /**
   * Simple augmented tree that updates file and byte counts for internal nodes during additions.
   */
  static class ZoneCheckerCountTree {
    private final int prefixLength;
    private final int countDepth;
    private final ZoneCheckerCountTreeNode root;

    ZoneCheckerCountTree(String basePath, int countDepth) {
      this.countDepth = countDepth;
      while (basePath.endsWith(String.valueOf(Path.SEPARATOR_CHAR))) {
        basePath = basePath.substring(0, basePath.length() - 1);
      }
      this.prefixLength = basePath.length();
      this.root = new ZoneCheckerCountTreeNode(null, basePath, null, 0, 0);
    }

    void addNode(String path, String replicationRule, long blockCount, long byteCount) {
      String[] components = StringUtils.split(
          path.substring(prefixLength + 1), Path.SEPARATOR_CHAR);
      // Trim until only countDepth left
      components = Arrays.copyOfRange(components, 0, Math.min(countDepth, components.length));
      this.root.addNode(components, replicationRule, blockCount, byteCount);
    }

    public Map<String, List<Long>> getMap() {
      Map<String, List<Long>> res = new HashMap<>();
      for (String distribution : root.getBlockCounts().keySet()) {
        res.put(distribution, Arrays.asList(
            root.getBlockCounts().get(distribution), root.getByteCounts().get(distribution)));
      }
      return res;
    }

    public ZoneCheckerCountTreeNode getRoot() {
      return root;
    }
  }

  /**
   * parallel checking using fork-join.
   */
  private static class CheckTask extends RecursiveAction {
    private final ZoneChecker zoneChecker;
    private final DistributedFileSystem dfs;
    private final Path fullPath;
    private final Map<ReplicationRule, Set<String>> ruleSetMap;
    private final Map<String, List<Long>> dcBlockStat;
    private final ZoneCheckerCountTree zcct;
    private final boolean blockSummaryFlag;
    private final boolean countOnly;
    private final float ratio;

    public CheckTask(ZoneChecker zoneChecker, DistributedFileSystem dfs, String fullPath,
        ConcurrentHashMap<ReplicationRule, Set<String>> ruleSetMap,
        Map<String, List<Long>> dcBlockStat, ZoneCheckerCountTree zcct,
        boolean blockSummaryFlag, boolean countOnly, float ratio) {
      this.zoneChecker = zoneChecker;
      this.dfs = dfs;
      this.fullPath = new Path(fullPath);
      this.ruleSetMap = ruleSetMap;
      this.dcBlockStat = dcBlockStat;
      this.zcct = zcct;
      this.blockSummaryFlag = blockSummaryFlag;
      this.countOnly = countOnly;
      this.ratio = ratio;
    }

    /**
     * Get all sub children of the full path.
     */
    private List<HdfsLocatedFileStatus> listPath() {
      List<HdfsLocatedFileStatus> hdfsFileStatuses = new ArrayList<>();
      try {
        RemoteIterator<LocatedFileStatus> iterator = dfs.listLocatedStatus(this.fullPath);
        while (iterator.hasNext()) {
          hdfsFileStatuses.add((HdfsLocatedFileStatus) iterator.next());
        }
      } catch (IOException e) {
        LOG.warn("Failed to list directory {}. Ignore the directory and continue.", fullPath, e);
      }

      if (hdfsFileStatuses.isEmpty()) {
        return hdfsFileStatuses;
      }

      int threshold = Math.max(MIN_FILE_NUM, Math.round(hdfsFileStatuses.size() * ratio));
      Collections.shuffle(hdfsFileStatuses);
      return hdfsFileStatuses.subList(0, threshold);
    }

    /**
     * All subtasks update results safely to avoid aggregate operation.
     */
    @Override
    public void compute() {
      List<HdfsLocatedFileStatus> children = listPath();
      ConcurrentHashMap<ReplicationRule, Set<String>> tmpRuleSetMap = new ConcurrentHashMap<>();
      if (!children.isEmpty()) {
        List<CheckTask> subtasks = new ArrayList<>();
        for (HdfsLocatedFileStatus child : children) {
          if (child.isDirectory()) {
            subtasks.add(new CheckTask(this.zoneChecker, dfs,
                child.getFullName(this.fullPath.toUri().getPath()),
                tmpRuleSetMap, this.dcBlockStat, this.zcct,
                this.blockSummaryFlag, this.countOnly, this.ratio));
          } else {
            this.zoneChecker.getReplicaInfoOfFile(fullPath.toUri().getPath(), child, tmpRuleSetMap,
                dcBlockStat, zcct, blockSummaryFlag, countOnly);
          }
        }
        // invoke and wait for completion
        invokeAll(subtasks);

        if (tmpRuleSetMap.size() == 1) {
          ReplicationRule rule = tmpRuleSetMap.keySet().iterator().next();
          tmpRuleSetMap.put(rule, new HashSet<>(Collections.singletonList(
              this.fullPath.toUri().getPath())));
        }

        synchronized (this) {
          tmpRuleSetMap.forEach((k, v) -> {
            Set<String> values = this.ruleSetMap.getOrDefault(k, new HashSet<>());
            values.addAll(v);
            this.ruleSetMap.put(k, values);
          });
        }
      }
    }
  }

  /**
   * Check replication distribution for a file.
   */
  private void getReplicaInfoOfFile(String parent, HdfsLocatedFileStatus fileStatus,
      Map<ReplicationRule, Set<String>> ruleSetMap, Map<String, List<Long>> dcBlockStat,
      ZoneCheckerCountTree zcct, boolean blockSummaryFlag, boolean countOnly) {
    if (!fileStatus.isSymlink()) { // ignore symlink
      String fullPath = fileStatus.getFullName(parent);
      short replication = fileStatus.getReplication();
      try {
        final LocatedBlocks locatedBlocks = fileStatus.getLocatedBlocks();
        final boolean lastBlkComplete = locatedBlocks.isLastBlockComplete();
        List<LocatedBlock> lbs = locatedBlocks.getLocatedBlocks();
        for (int i = 0; i < lbs.size(); i++) {
          if (i == lbs.size() - 1 && !lastBlkComplete) {
            // last block is incomplete, skip it
            continue;
          }
          LocatedBlock lb = lbs.get(i);
          long blockSize = lb.getBlockSize();
          Map<String, Short> mapDCReplica = ZoneMover.getBlockDistribution(lb);
          if (lb.getLocations().length != replication || lb.getLocations().length > 5) {
            LOG.debug("Found the abnormal file {} with replication {} and distribution {}",
                fullPath, replication, ReplicationRule.parseFromMap(mapDCReplica));
          }
          // Aggregate the number of blocks and the total size of blocks in each DC
          if (blockSummaryFlag) {
            synchronized (this) {
              for (String dc : mapDCReplica.keySet()) {
                short replicas = mapDCReplica.get(dc);
                if (dcBlockStat.containsKey(dc)) {
                  dcBlockStat.get(dc).set(0, dcBlockStat.get(dc).get(0) + replicas);
                  dcBlockStat.get(dc).set(1, dcBlockStat.get(dc).get(1) + blockSize * replicas);
                } else {
                  dcBlockStat.put(dc, Arrays.asList((long) replicas, blockSize * replicas));
                }
              }
            }
          } else {
            ReplicationRule replicationRule = ReplicationRule.parseFromMap(mapDCReplica);
            // Aggregate the number of blocks and the total size of blocks
            // in each rule for each iNodes.
            if (countOnly) {
              if (zcct == null) {
                continue;
              }
              zcct.addNode(fullPath, replicationRule.toString(), 1, lb.getBlockSize());
            } else {
              synchronized (this) {
                // Aggregate paths in different rule.
                if (ruleSetMap.containsKey(replicationRule)) {
                  ruleSetMap.get(replicationRule).add(fullPath);
                } else {
                  ruleSetMap.put(replicationRule, new HashSet<>(Collections.singletonList(fullPath)));
                }
              }
            }
          }
        }
      } catch (Exception e) {
        LOG.warn("Failed to check the status of {}. Ignore it and continue.", parent, e);
      }
    }
  }

  /**
   * A tree node for {@link ZoneCheckerCountTree}.
   * Augmented with block and byte count of all its children, recursively.
   */
  static class ZoneCheckerCountTreeNode {
    private final Map<String, Long> blockCounts;
    private final Map<String, Long> byteCounts;
    final ZoneCheckerCountTreeNode parent;
    final Map<String, ZoneCheckerCountTreeNode> children = new HashMap<>();
    final String name;

    ZoneCheckerCountTreeNode(ZoneCheckerCountTreeNode parent, String name,
        String replicationRule, long blockCount, long byteCount) {
      this.parent = parent;
      this.blockCounts = new HashMap<>();
      this.byteCounts = new HashMap<>();
      this.name = name;
      if (replicationRule != null) {
        this.blockCounts.put(replicationRule, blockCount);
        this.byteCounts.put(replicationRule, byteCount);
      }
    }

    synchronized Map<String, Long> getBlockCounts() {
      return new HashMap<>(this.blockCounts);
    }

    synchronized Map<String, Long> getByteCounts() {
      return new HashMap<>(this.byteCounts);
    }

    /**
     * Add the number of blocks and the bytes of blocks.
     */
    private void updateCounters(String replicationRule, long blockCount, long byteCount) {
      long blocks = blockCounts.getOrDefault(replicationRule, 0L);
      blockCounts.put(replicationRule, blocks + blockCount);

      long bytes = byteCounts.getOrDefault(replicationRule, 0L);
      byteCounts.put(replicationRule, bytes + byteCount);
    }

    /**
     * Add a new child.
     */
    private void addChild(ZoneCheckerCountTreeNode child) {
      children.put(child.name, child);
    }

    /**
     * Try to add some children.
     */
    synchronized void addNode(String[] components,
        String replRule, long blockCount, long byteCount) {
      // No recursion case
      if (components.length == 0) {
        this.updateCounters(replRule, blockCount, byteCount);
        return;
      }

      String childName = components[0];
      // Leaf node
      if (components.length == 1) {
        // Existing leaf node
        if (children.containsKey(childName)) {
          children.get(childName).updateCounters(replRule, blockCount, byteCount);
        } else {
          // New leaf node
          addChild(new ZoneCheckerCountTreeNode(this, childName, replRule, blockCount, byteCount));
        }
        this.updateCounters(replRule, blockCount, byteCount);
        return;
      }
      // Internal node
      // New subtree
      if (!children.containsKey(childName)) {
        updateCounters(replRule, blockCount, byteCount);
        ZoneCheckerCountTreeNode parent = this;
        for (String component : components) {
          ZoneCheckerCountTreeNode node =
              new ZoneCheckerCountTreeNode(parent, component, replRule, blockCount, byteCount);
          parent.addChild(node);
          parent = node;
        }
      } else {
        // Existing node, just add the block and byte count then pass the job to child
        updateCounters(replRule, blockCount, byteCount);
        children.get(childName)
            .addNode(Arrays.copyOfRange(components, 1, components.length), replRule, blockCount,
                byteCount);
      }
    }
  }
}
