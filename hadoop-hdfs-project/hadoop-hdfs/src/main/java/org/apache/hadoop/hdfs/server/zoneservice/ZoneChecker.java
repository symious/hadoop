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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ZoneChecker {
  private static final Logger LOG = LoggerFactory.getLogger(ZoneChecker.class);
  private static final String ROOT = "/";
  private final DFSClient dfs;

  ZoneChecker(NameNodeConnector nnc, Configuration conf) {
    Dispatcher dispatcher = new ZoneDispatcher(
        nnc, Collections.emptySet(),
        Collections.emptySet(), 0, 0,
        0, conf, 0, 0);
    this.dfs = dispatcher.getDistributedFileSystem().getClient();
  }

  /**
   * Run with prepared arguments.
   * @param conf configuration
   * @param path the path to be checked
   */
  private static int run(Configuration conf, URI nameNode, String path) {
    LOG.info("Start to check path: " + path);
    NameNodeConnector nnc;
    try {
      nnc = new NameNodeConnector(nameNode,
          Collections.singletonList(new Path(path)),
          conf, 1);
      final ZoneChecker zch = new ZoneChecker(nnc, conf);
      Map<ReplicationRule, List<String>> result =
          zch.getReplicaInfo(path);
      printResult(result);
      return 0;
    } catch (IOException e) {
      LOG.warn("ZoneChecker failed check {} from {}", path, nameNode, e);
      return 1;
    }
  }

  static class Cli extends Configured implements Tool {
    private static final String USAGE = "Usage: hdfs zonechecker"
        + "\n\t[-namespace <namespace>]\tthe namespace to be checked."
        + "\n\t-path <path>\tthe path to be checked.";

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

    private static String getPath(CommandLine line)
        throws IllegalArgumentException {
      String path = line.getOptionValue("path");
      if (!path.startsWith(ROOT)) {
        throw new IllegalArgumentException("Please provide a valid path!");
      }
      return path;
    }

    @Override
    public int run(String[] args) {
      final Configuration conf = getConf();
      final Options options = buildCliOptions();
      CommandLineParser parser = new GnuParser();

      try {
        CommandLine commandLine = parser.parse(options, args, true);
        return run(conf, getNamespaceUri(commandLine, conf),
            getPath(commandLine));
      } catch (ParseException | IllegalArgumentException e) {
        System.out.println(e + ".  Exiting ...");
        return ExitStatus.ILLEGAL_ARGUMENTS.getExitCode();
      } finally {
        System.out.format("%-24s ",
            DateFormat.getDateTimeInstance().format(new Date()));
      }
    }

    /**
     * Run with prepared arguments.
     */
    int run(Configuration conf, URI namenodeURI, String path) {
      return ZoneChecker.run(conf, namenodeURI, path);
    }
  }

  public Map<ReplicationRule, List<String>> getReplicaInfo(String fullPath) {
    Map<ReplicationRule, List<String>> resultMap = new HashMap<>();
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
      for (HdfsFileStatus child : children.getPartialListing()) {
        resultMap = mergeRules(resultMap,
            getReplicaInfoRecursively(fullPath, child));
      }

      if (resultMap.keySet().size() == 1) {
        resultMap.put(resultMap.keySet().iterator().next(),
            new ArrayList<>(Collections.singletonList(fullPath)));
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
  private Map<ReplicationRule, List<String>> getReplicaInfoRecursively(
      String parent, HdfsFileStatus status) {
    Map<ReplicationRule, List<String>> resultMap = new HashMap<>();
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
                new ArrayList<>(Collections.singletonList(fullPath)));
          }
        }
      } catch (Exception e) {
        LOG.warn("Failed to check the status of " + parent
            + ". Ignore it and continue.", e);
      }
    }
    return resultMap;
  }

  private Map<ReplicationRule, List<String>> mergeRules(
      Map<ReplicationRule, List<String>> rule1,
      Map<ReplicationRule, List<String>> rule2) {
    if (rule1.isEmpty()) {
      return rule2;
    }
    for (ReplicationRule key : rule2.keySet()) {
      if (rule1.containsKey(key)) {
        rule1.get(key).addAll(rule2.get(key));
      } else {
        rule1.put(key, rule2.get(key));
      }
    }
    return rule1;
  }

  private static void printResult(Map<ReplicationRule, List<String>> map) {
    System.out.println("Zone checker result: ");
    for (ReplicationRule replicationRule: map.keySet()) {
      System.out.println(replicationRule.toString() + ":");
      for (String path: map.get(replicationRule)) {
        System.out.println("  " + path);
      }
    }
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