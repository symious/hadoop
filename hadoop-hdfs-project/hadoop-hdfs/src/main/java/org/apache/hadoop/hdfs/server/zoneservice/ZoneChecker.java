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

  public ZoneChecker(NameNodeConnector nnc, Configuration conf) {
    Dispatcher dispatcher = new ZoneDispatcher(
        nnc, Collections.emptySet(),
        Collections.emptySet(), 0, 0,
        0, conf, 0, 0);
    ratio = conf.getFloat(DFSConfigKeys.DFS_ZONECHECKER_DEFAULT_RATIO,
        DFSConfigKeys.DFS_ZONECHECKER_DEFAULT_RATIO_DEFAULT);
    this.dfs = dispatcher.getDistributedFileSystem().getClient();
  }

  /**
   * Run with prepared arguments.
   * @param conf configuration
   * @param path the path to be checked
   */
  private static int run(Configuration conf,
      URI nameNode, String path, Float ratio) {
    LOG.info("Start to check path: " + path);
    NameNodeConnector nnc;
    try {
      nnc = new NameNodeConnector(nameNode,
          Collections.singletonList(new Path(path)),
          conf, 1);
      final ZoneChecker zch = new ZoneChecker(nnc, conf);
      //if ratio is inputted by user, set it
      if (ratio <= 0.0f) {
        LOG.info("Start to check path: " + path + " with default ratio.");
      } else {
        LOG.info("Start to check path: " + path + " with ratio " + ratio);
        zch.setRatio(ratio);
      }
      Map<ReplicationRule, Set<String>> result = zch.getReplicaInfo(path);
      printResult(result);
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
          Collections.singletonList(new Path(path)), conf, 1);
      final ZoneChecker zch = new ZoneChecker(nnc, conf);
      //if ratio is inputted by user, set it
      if (ratio > 0.0f) {
        zch.setRatio(ratio);
      }
      return zch.getReplicaInfo(path);
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  static class Cli extends Configured implements Tool {
    private static final String USAGE = "Usage: hdfs zonechecker"
        + "\n\t[-namespace <namespace>]\tthe namespace to be checked."
        + "\n\t-path <path>\tthe path to be checked."
        + "\n\t[-ratio <ratio>]\tif the path is a directory, the ratio of "
        + "files will be checked";

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

    @Override
    public int run(String[] args) {
      long startTime = Time.monotonicNow();
      final Configuration conf = getConf();
      final Options options = buildCliOptions();
      CommandLineParser parser = new GnuParser();
      CommandLine commandLine;
      try {
        commandLine = parser.parse(options, args, true);
        return run(conf, getNamespaceUri(commandLine, conf),
            getPath(commandLine), getRatio(commandLine));
      } catch (ParseException | IllegalArgumentException e) {
        System.out.println(e + ".  Exiting ...");
        return ExitStatus.ILLEGAL_ARGUMENTS.getExitCode();
      } finally {
        System.out.format("ZoneChecker took "
            + StringUtils.formatTime(Time.monotonicNow() - startTime));
      }
    }

    /**
     * Run with given ratio.
     */
    int run(Configuration conf, URI namenodeURI,
        String path, Float ratio) {
      return ZoneChecker.run(conf, namenodeURI, path, ratio);
    }
  }

  public Map<ReplicationRule, Set<String>> getReplicaInfo(String fullPath) {
    Map<ReplicationRule, Set<String>> resultMap = new HashMap<>();
    byte[] lastReturnedName = HdfsFileStatus.EMPTY_NAME;
    while (true) {
      final DirectoryListing children;
      try {
        children = dfs.listPaths(fullPath, lastReturnedName, true);
      } catch(IOException e) {
        LOG.warn("Failed to list directory " + fullPath
            + ". Ignore the directory and continue.", e);
        return resultMap;
      }
      if (children == null) {
        return resultMap;
      }
      HdfsFileStatus[] partialList = children.getPartialListing();
      int threshold = Math.max(MIN_FILE_NUM,
          Math.round(partialList.length * ratio));
      for (HdfsFileStatus child : getRandomList(partialList, threshold)) {
        resultMap = mergeRules(resultMap,
            getReplicaInfoRecursively(fullPath, child));
      }

      if (resultMap.keySet().size() == 1) {
        resultMap.put(resultMap.keySet().iterator().next(),
            new HashSet<>(Collections.singletonList(fullPath)));
      }

      if (children.hasMore()) {
        lastReturnedName = children.getLastName();
      } else {
        break;
      }
    }
    return resultMap;
  }

  /** @return whether the check requires next round */
  private Map<ReplicationRule, Set<String>> getReplicaInfoRecursively(
      String parent, HdfsFileStatus status) {
    Map<ReplicationRule, Set<String>> resultMap = new HashMap<>();
    String fullPath = status.getFullName(parent);
    if (status.isDir()) {
      if (!fullPath.endsWith(Path.SEPARATOR)) {
        fullPath = fullPath + Path.SEPARATOR;
      }

      resultMap = getReplicaInfo(fullPath);
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
          Map<String, Short> MapDCReplica = ZoneMover.getBlockDistribution(lb);
          ReplicationRule replicationRule =
              ReplicationRule.parseFromMap(MapDCReplica);
          if (resultMap.containsKey(replicationRule)) {
            resultMap.get(replicationRule).add(fullPath);
          } else {
            resultMap.put(replicationRule,
                new HashSet<>(Collections.singletonList(fullPath)));
          }
        }
      } catch (Exception e) {
        LOG.warn("Failed to check the status of " + parent
            + ". Ignore it and continue.", e);
      }
    }
    return resultMap;
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

  private Map<ReplicationRule, Set<String>> mergeRules(
      Map<ReplicationRule, Set<String>> rule1,
      Map<ReplicationRule, Set<String>> rule2) {
    if (rule1.isEmpty()) { return rule2; }
    for (Map.Entry<ReplicationRule, Set<String>> entry:rule2.entrySet()) {
      ReplicationRule key = entry.getKey();
      List<ReplicationRule> keyList = new ArrayList<>(rule1.keySet());
      if (keyList.contains(key)) {
        rule1.get(key).addAll(entry.getValue());
      } else {
        rule1.put(key, entry.getValue());
      }
    }
    return rule1;
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