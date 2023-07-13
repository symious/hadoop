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
package org.apache.hadoop.hdfs.server.balancer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.util.HostsFileReader;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.checkArgument;

public class DecommissionBalancer extends Balancer {
  static final Logger LOG = LoggerFactory.getLogger(DecommissionBalancer.class);

  private static final String USAGE = "Usage: hdfs decommissionbalancer"
      + "\n\t[-threshold <threshold>]\tPercentage of disk capacity"
      + "\n\t[-exclude [-f <hosts-file> | <comma-separated list of hosts>]]"
      + "\tExcludes the specified datanodes."
      + "\n\t[-include [-f <hosts-file> | <comma-separated list of hosts>]]"
      + "\tIncludes only the specified datanodes."
      + "\n\t[-blockpools <comma-separated list of blockpool ids>]"
      + "\tThe decommission balancer will only run on blockpools included in this list."
      + "\n\t[-idleiterations <idleiterations>]"
      + "\tNumber of consecutive idle iterations (-1 for Infinite) before "
      + "exit."
      + "\n\t[-runDuringUpgrade]"
      + "\tWhether to run the decommission balancer during an ongoing HDFS upgrade."
      + "This is usually not desired since it will not affect used space "
      + "on over-utilized machines."
      + "\n\t[-dataCenterConstraint <data-center-constraint>]"
      + "\tConstraint balance scope into specific data center."
      + "\n\t[-asService]\tRun as a long running service.";
  private final BalancingPolicy policy;

  private final Collection<Dispatcher.Source> sourceList = new LinkedList<Dispatcher.Source>();
  private final Collection<Dispatcher.DDatanode.StorageGroup> belowAvgUtilized
      = new LinkedList<Dispatcher.DDatanode.StorageGroup>();
  private final Collection<Dispatcher.DDatanode.StorageGroup> underUtilized
      = new LinkedList<Dispatcher.DDatanode.StorageGroup>();
  private final Collection<Dispatcher.DDatanode.StorageGroup> leftNodes
      = new LinkedList<Dispatcher.DDatanode.StorageGroup>();
  /** historyMatcher will record the history target for the source */
  private final Map<Dispatcher.DDatanode.StorageGroup, List<Dispatcher.DDatanode.StorageGroup>>
      historyMatcher;
  private String dataCenterConstraint = null;
  private String targetDataCenter = null;
  static final Path DECOMMISSION_BALANCER_ID_PATH =
      new Path("/system/decommission_balancer.id");

  DecommissionBalancer(NameNodeConnector nnc, BalancerParameters p,
      Configuration conf) {
    this(nnc, p, conf, new HashMap<>());
  }

  /**
   * Construct a decommission balancer.
   * Initialize balancer. It sets the value of the threshold, and
   * builds the communication proxies to
   * namenode as a client and a secondary namenode and retry proxies
   * when connection fails.
   */
  DecommissionBalancer(NameNodeConnector nnc, BalancerParameters p,
      Configuration conf, Map<Dispatcher.DDatanode.StorageGroup, List<Dispatcher.DDatanode.StorageGroup>>
      historyMatcher) {
    super(nnc, p, conf);
    this.policy = BalancingPolicy.Decommission.INSTANCE;
    this.historyMatcher =  historyMatcher;
  }

  /**
   * Construct a decommission balancer.
   * Initialize balancer and constraint the source DC and target DC inside decommission balancer
   */
  DecommissionBalancer(NameNodeConnector nnc, BalancerParameters p,
      Configuration conf, Map<Dispatcher.DDatanode.StorageGroup, List<Dispatcher.DDatanode.StorageGroup>>
      historyMatcher, String dataCenterConstraint) {
    super(nnc, p, conf);
    this.policy = BalancingPolicy.Decommission.INSTANCE;
    this.historyMatcher =  historyMatcher;
    this.dataCenterConstraint = dataCenterConstraint;
    this.targetDataCenter = p.getTargetDataCenter();
  }

  @Override
  protected long init(List<DatanodeStorageReport> reports) {
    long sizeToMove = 0L;
    // compute average utilization
    for (DatanodeStorageReport r : reports) {
      policy.accumulateSpaces(r);
    }
    policy.initAvgUtilization();
    for(DatanodeStorageReport r : reports) {
      final Dispatcher.DDatanode dn = dispatcher.newDatanode(r.getDatanodeInfo());
      boolean isDecommissioning = r.getDatanodeInfo().isDecommissionInProgress();

      for(StorageType t : StorageType.getMovableTypes()) {
        // decommissioning node will be considered as source
        if (isDecommissioning) {
          // if data center is set inside DecommissionBalancer,
          // filter the source according to the constraint
          if (dataCenterConstraint != null) {
            if (!dn.getDatanodeInfo().getNetworkLocation().startsWith(dataCenterConstraint)) {
              continue;
            }
          }
          final Dispatcher.Source s = dn.addSource(t, getUsed(r, t), dispatcher);
          sizeToMove += getUsed(r, t);

          if (!sourceList.contains(s)) {
            sourceList.add(s);
            dispatcher.getStorageGroupMap().put(s);
          }
          continue;
        }

        // If target data center is set, filter target nodes are not in the target DC
        if (targetDataCenter != null) {
          if (!dn.getDatanodeInfo().getNetworkLocation().startsWith(targetDataCenter)) {
            continue;
          }
        }

        final Double utilization = policy.getUtilization(r, t);
        if (utilization == null) { // datanode does not have such storage type
          // TODO: Need to handle the case that datanode does not have such storage type
          continue;
        }

        final double average = policy.getAvgUtilization(t);
        LOG.debug("Node {} usage percentage is {}!", r, average);
        if (average >= 100) {
          LOG.error("Storage type {} may not handle the decommission. Total used: {}," +
              " Total capacity: {}", t, policy.totalUsedSpaces, policy.totalCapacities);
          continue;
        }
        if (utilization >= average) {
          LOG.warn(dn + "[" + t + "] has utilization=" + utilization
              + " >= average=" + average);
          // Still add the high utilization node into backup target list
          leftNodes.add(dn.addTarget(t, getRemaining(r,t) / 10));
          continue;
        }

        final double utilizationDiff = utilization - average;
        final long capacity = getCapacity(r, t);
        final double thresholdDiff = Math.abs(utilizationDiff) - threshold;
        final long maxSize2Move = computeMaxSize2Move(capacity,
            getRemaining(r, t), utilizationDiff, maxSizeToMove);

        final Dispatcher.DDatanode.StorageGroup g;

        g = dn.addTarget(t, maxSize2Move);
        if (thresholdDiff <= 0) { // within threshold
          belowAvgUtilized.add(g);
        } else {
          underUtilized.add(g);
        }

        dispatcher.getStorageGroupMap().put(g);
      }
    }

    return sizeToMove;
  }

  @Override
  // TODO: When all replicas of one block is decommissioning at the same time,
  //  all the replicas may be moved to the same node;
  protected void chooseStorageGroups(final Matcher matcher) {
    LOG.info("chooseStorageGroups for " + matcher + ": decommission => underUtilized" + " number is " + underUtilized.size());
    chooseStorageGroups(sourceList, underUtilized, matcher);

    LOG.info("chooseStorageGroups for " + matcher + ": decommission => belowAvgUtilized" + " number is " + belowAvgUtilized.size());
    chooseStorageGroups(sourceList, belowAvgUtilized, matcher);

    LOG.info("chooseStorageGroups for " + matcher + ": decommission => leftNodes" + " number is " + leftNodes.size());
    chooseStorageGroups(sourceList, leftNodes, matcher);
  }

  <G extends Dispatcher.DDatanode.StorageGroup, C extends Dispatcher.DDatanode.StorageGroup>
  void chooseStorageGroups(Collection<G> groups, Collection<C> candidates,
      Matcher matcher) {
    for(final Iterator<G> i = groups.iterator(); i.hasNext();) {
      final G g = i.next();
      for(; choose4One(g, candidates, matcher); );
      if (!g.hasSpaceForScheduling()) {
        i.remove();
      }
    }
  }

  /**
   * For the given datanode, choose a candidate and then schedule it.
   * @return true if a candidate is chosen; false if no candidates is chosen.
   */
  @Override
  protected <C extends Dispatcher.DDatanode.StorageGroup> boolean choose4One(
      Dispatcher.DDatanode.StorageGroup g,
      Collection<C> candidates, Matcher matcher) {
    final Iterator<C> i = candidates.iterator();
    final C chosen = chooseCandidate(g, i, matcher);

    if (chosen == null) {
      return false;
    }
    if (g instanceof Dispatcher.Source) {
      if (!historyMatcher.containsKey(g)) {
        historyMatcher.put(g, new ArrayList<>());
      }
      historyMatcher.get(g).add(chosen);
      matchSourceWithTargetToMove((Dispatcher.Source)g, chosen);
    } else {
      if (!historyMatcher.containsKey(chosen)) {
        historyMatcher.put(chosen, new ArrayList<>());
      }
      historyMatcher.get(chosen).add(g);
      matchSourceWithTargetToMove((Dispatcher.Source)chosen, g);
    }
    if (!chosen.hasSpaceForScheduling()) {
      i.remove();
    }
    return true;
  }

  /** Choose a candidate for the given datanode. */
  @Override
  protected <G extends Dispatcher.DDatanode.StorageGroup, C extends Dispatcher.DDatanode.StorageGroup>
  C chooseCandidate(G g, Iterator<C> candidates, Matcher matcher) {
    if (g.hasSpaceForScheduling()) {
      for(; candidates.hasNext(); ) {
        final C c = candidates.next();
        if (g instanceof Dispatcher.Source) {
          if (historyMatcher.get(g) != null) {
            if (historyMatcher.get(g).contains(c)) {
              continue;
            }
          }
        } else {
          if (historyMatcher.get(c) != null) {
            if (historyMatcher.get(c).contains(g)) {
              continue;
            }
          }
        }
        if (!c.hasSpaceForScheduling()) {
          candidates.remove();
        } else if (matchStorageGroups(c, g, matcher)) {
          return c;
        }
      }
    }
    return null;
  }

  @Override
  Result runOneIteration() {
    try {
      int decommissioningNodesCount = dispatcher.countDecommissioningNode();
      if (decommissioningNodesCount == 0) {
        LOG.info("There is no node in decommissioning!");
        return newResult(ExitStatus.SUCCESS, 0, 0);
      }
      final List<DatanodeStorageReport> reports = dispatcher.init(true);
      final long bytesLeftToMove = init(reports);

      LOG.info( "Need to decommission "+ decommissioningNodesCount
          + " DNs to finish the current decommission." );

      /* Decide all the nodes that will participate in the block move and
       * the number of bytes that need to be moved from one node to another
       * in this iteration. Maximum bytes to be moved per node is
       * Min(1 Band worth of bytes,  MAX_SIZE_TO_MOVE).
       */
      final long bytesBeingMoved = chooseStorageGroups();
      if (bytesBeingMoved == 0) {
        System.out.println("No block can be moved. Exiting...");
        return newResult(ExitStatus.NO_MOVE_BLOCK, bytesLeftToMove, bytesBeingMoved);
      } else {
        LOG.info("Will move {}  in this iteration for {}",
            StringUtils.byteDesc(bytesBeingMoved), nnc.toString());
      }

      /* For each pair of <source, target>, start a thread that repeatedly
       * decide a block to be moved and its proxy source,
       * then initiates the move until all bytes are moved or no more block
       * available to move.
       * Exit no byte has been moved for 5 consecutive iterations.
       */
      if (!dispatcher.dispatchAndCheckContinue()) {
        return newResult(ExitStatus.NO_MOVE_PROGRESS, bytesLeftToMove, bytesBeingMoved);
      }

      return newResult(ExitStatus.IN_PROGRESS, bytesLeftToMove, bytesBeingMoved);
    } catch (IllegalArgumentException e) {
      System.out.println(e + ".  Exiting ...");
      return newResult(ExitStatus.ILLEGAL_ARGUMENTS);
    } catch (IOException e) {
      System.out.println(e + ".  Exiting ...");
      return newResult(ExitStatus.IO_EXCEPTION);
    } catch (InterruptedException e) {
      System.out.println(e + ".  Exiting ...");
      return newResult(ExitStatus.INTERRUPTED);
    } finally {
      dispatcher.shutdownNow();
    }
  }

  static private int doMigration(Collection<URI> namenodes,
      Collection<String> nsIds, final BalancerParameters p, Configuration conf)
      throws IOException, InterruptedException {
    // TODO: check all the NNs when different NNs has different status, refer to the below flag
    boolean checkAllNNs = conf.getBoolean(
        DFSConfigKeys.DFS_DECOMMISSION_BALANCER_CHECK_ALL_NAMENODE_KEY,
        DFSConfigKeys.DFS_DECOMMISSION_BALANCER_CHECK_ALL_NAMENODE_DEFAULT);
    final long sleeptime =
        conf.getTimeDuration(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
            DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT,
            TimeUnit.SECONDS, TimeUnit.MILLISECONDS) * 2 +
            conf.getTimeDuration(
                DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY,
                DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_DEFAULT,
                TimeUnit.SECONDS, TimeUnit.MILLISECONDS);
    LOG.info("namenodes  = " + namenodes);
    LOG.info("parameters = " + p);
    LOG.info("included nodes = " + p.getIncludedNodes());
    LOG.info("excluded nodes = " + p.getExcludedNodes());
    LOG.info("data center constraint = " + p.getDataCenterConstraint());
    LOG.info("target data center = " + p.getTargetDataCenter());
    checkKeytabAndInit(conf);
    System.out.println("Time Stamp               Iteration#"
        + "  Bytes Already Moved  Bytes Left To Move  Bytes Being Moved"
        + "  NameNode");

    List<NameNodeConnector> connectors = new ArrayList<>();
    try {
      if (checkAllNNs) {
        for (URI namenode : namenodes) {
          Map<String, InetSocketAddress> addresses =
              DFSUtilClient.getHaNnRpcAddresses(conf).get(namenode.getAuthority());
          for (InetSocketAddress address : addresses.values()) {
            LOG.debug("Add the address: {} into the address list", address.toString());
            Path path = new Path(
                getBalancePathForMultiDC(p.getDataCenterConstraint()).toString() + "-" +
                    address.toString().split("/")[0]);
            NameNodeConnector specificnnc =
                new NameNodeConnector(DecommissionBalancer.class.getSimpleName(),
                    namenode, address, namenode.getAuthority(), path,
                    null, conf, p.getMaxIdleIteration());
            connectors.add(specificnnc);
          }
        }
      } else {
        connectors = NameNodeConnector.newNameNodeConnectors(namenodes, nsIds,
            DecommissionBalancer.class.getSimpleName(),
            getBalancePathForMultiDC(p.getDataCenterConstraint()),
            conf, p.getMaxIdleIteration());
      }

      LOG.debug("Namenode list is {}", connectors);
      boolean done = false;
      final Map<Dispatcher.DDatanode.StorageGroup, List<Dispatcher.DDatanode.StorageGroup>>
          historyMatcher = new HashMap();
      for(int iteration = 0; !done; iteration++) {
        done = true;
        Collections.shuffle(connectors);
        for(NameNodeConnector nnc : connectors) {
          if (p.getBlockPools().size() == 0
              || p.getBlockPools().contains(nnc.getBlockpoolID())) {
            // Check every block regardless of its size
            conf.setLong(DFSConfigKeys.DFS_BALANCER_GETBLOCKS_MIN_BLOCK_SIZE_KEY, 1);
            final DecommissionBalancer b;
            // If target DC is set, release the DC constraint
            if (p.getTargetDataCenter() != null) {
              String dcConstraint = p.getDataCenterConstraint();
              p.setDataCenterConstraint("/");
              b = new DecommissionBalancer(nnc, p, conf, historyMatcher, dcConstraint);
            } else {
              b = new DecommissionBalancer(nnc, p, conf, historyMatcher);
            }
            final Result r = b.runOneIteration();
            r.print(iteration, nnc, System.out);

            // clean all lists
            b.resetData(conf);

            if (r.getExitStatus() == ExitStatus.SUCCESS) {
              LOG.debug("Decommission on NN {} is done!", nnc);
            } else {
              if (r.getExitStatus() == ExitStatus.IN_PROGRESS) {
                done = false;
              } // no block can be moved but decommissioning hasn't done, try the previous match
              else if (r.getExitStatus() == ExitStatus.NO_MOVE_BLOCK) {
                LOG.info("Clean all the history matcher, retry all the possibility");
                historyMatcher.clear();
                done = false;
              } else {
                return r.getExitStatus().getExitCode();
              }
            }
          } else {
            LOG.info("Skipping blockpool " + nnc.getBlockpoolID());
          }
          if (done) {
            System.out.println("The decommissioning process is done for " + nnc + ". Exiting...");
          }
        }
        if (!done) {
          Thread.sleep(sleeptime);
        }
      }
    } finally {
      for(NameNodeConnector nnc : connectors) {
        IOUtils.cleanupWithLogger(LOG, nnc);
      }
    }
    return ExitStatus.SUCCESS.getExitCode();
  }

  private static void checkKeytabAndInit(Configuration conf)
      throws IOException {
    if (conf.getBoolean(DFSConfigKeys.DFS_BALANCER_KEYTAB_ENABLED_KEY,
        DFSConfigKeys.DFS_BALANCER_KEYTAB_ENABLED_DEFAULT)) {
      UserGroupInformation.setConfiguration(conf);
      String addr = conf.get(DFSConfigKeys.DFS_DECOMMISSION_BALANCER_ADDRESS_KEY,
          DFSConfigKeys.DFS_DECOMMISSION_BALANCER_ADDRESS_DEFAULT);
      InetSocketAddress socAddr = NetUtils.createSocketAddr(addr, 0,
          DFSConfigKeys.DFS_DECOMMISSION_BALANCER_ADDRESS_KEY);
      SecurityUtil.login(conf, DFSConfigKeys.DFS_BALANCER_KEYTAB_FILE_KEY,
          DFSConfigKeys.DFS_BALANCER_KERBEROS_PRINCIPAL_KEY,
          socAddr.getHostName());
    }
  }

  static int run(Collection<URI> namenodes, Collection<String> nsIds,
      final BalancerParameters p, Configuration conf)
      throws IOException, InterruptedException {
    if (!p.getRunAsService()) {
      return doMigration(namenodes, nsIds, p, conf);
    }
    if (!serviceRunning) {
      serviceRunning = true;
    } else {
      LOG.warn("Decommission Balancer already running as a long-service!");
      return ExitStatus.ALREADY_RUNNING.getExitCode();
    }

    long scheduleInterval = conf.getTimeDuration(
        DFSConfigKeys.DFS_BALANCER_SERVICE_INTERVAL_KEY,
        DFSConfigKeys.DFS_BALANCER_SERVICE_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS);
    int retryOnException =
        conf.getInt(DFSConfigKeys.DFS_BALANCER_SERVICE_RETRIES_ON_EXCEPTION,
            DFSConfigKeys.DFS_BALANCER_SERVICE_RETRIES_ON_EXCEPTION_DEFAULT);

    while (serviceRunning) {
      try {
        int retCode = doMigration(namenodes, nsIds, p, conf);
        if (retCode < 0) {
          LOG.info("Decommission failed, error code: " + retCode);
          failedTimesSinceLastSuccessfulBalance++;
        } else {
          LOG.info("Decommission succeed!");
          failedTimesSinceLastSuccessfulBalance = 0;
        }
        exceptionsSinceLastBalance = 0;
      } catch (Exception e) {
        if (++exceptionsSinceLastBalance > retryOnException) {
          // The caller will process and log the exception
          throw e;
        }
        LOG.warn(
            "Encounter exception while do decommission work. Already tried {} times",
            exceptionsSinceLastBalance, e);
      }

      // sleep for next round, will retry for next round when it's interrupted
      LOG.info("Finished one round, will wait for {} for next round",
          time2Str(scheduleInterval));
      Thread.sleep(scheduleInterval);
    }
    // normal stop
    return 0;
  }

  static long getUsed(DatanodeStorageReport report, StorageType t) {
    long used = 0L;
    for(StorageReport r : report.getStorageReports()) {
      if (r.getStorage().getStorageType() == t) {
        used += r.getDfsUsed();
      }
    }
    return used;
  }

  public static Path getBalancePathForMultiDC(String dataCenterConstraint) {
    if (dataCenterConstraint != null && !dataCenterConstraint.isEmpty()) {
      return new Path(DECOMMISSION_BALANCER_ID_PATH, dataCenterConstraint.substring(1));
    }
    return DECOMMISSION_BALANCER_ID_PATH;
  }

  static class Cli extends Configured implements Tool {
    /**
     * Parse arguments and then run Decommission Balancer.
     *
     * @param args command specific arguments.
     * @return exit code. 0 indicates success, non-zero indicates failure.
     */
    @Override
    public int run(String[] args) {
      final long startTime = Time.monotonicNow();
      final Configuration conf = getConf();

      try {
        checkReplicationPolicyCompatibility(conf);

        final Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf);
        final Collection<String> nsIds = DFSUtilClient.getNameServiceIds(conf);
        return DecommissionBalancer.run(namenodes, nsIds, parse(args), conf);
      } catch (IOException e) {
        System.out.println(e + ".  Exiting ...");
        return ExitStatus.IO_EXCEPTION.getExitCode();
      } catch (InterruptedException e) {
        System.out.println(e + ".  Exiting ...");
        return ExitStatus.INTERRUPTED.getExitCode();
      } finally {
        System.out.format("%-24s ",
            DateFormat.getDateTimeInstance().format(new Date()));
        System.out.println("Decommission migrating took "
            + time2Str(Time.monotonicNow() - startTime));
      }
    }

    /** parse command line arguments */
    static BalancerParameters parse(String[] args) {
      Set<String> excludedNodes = null;
      Set<String> includedNodes = null;
      boolean isSetDataCenter = false;
      BalancerParameters.Builder b = new BalancerParameters.Builder();

      if (args != null) {
        try {
          for(int i = 0; i < args.length; i++) {
            if ("-threshold".equalsIgnoreCase(args[i])) {
              checkArgument(++i < args.length,
                  "Threshold value is missing: args = " + Arrays.toString(args));
              try {
                double threshold = Double.parseDouble(args[i]);
                if (threshold < 1 || threshold > 100) {
                  throw new IllegalArgumentException(
                      "Number out of range: threshold = " + threshold);
                }
                LOG.info( "Using a threshold of " + threshold );
                b.setThreshold(threshold);
              } catch(IllegalArgumentException e) {
                System.err.println(
                    "Expecting a number in the range of [1.0, 100.0]: "
                        + args[i]);
                throw e;
              }
            } else if ("-exclude".equalsIgnoreCase(args[i])) {
              excludedNodes = new HashSet<>();
              i = processHostList(args, i, "exclude", excludedNodes);
              b.setExcludedNodes(excludedNodes);
            } else if ("-include".equalsIgnoreCase(args[i])) {
              includedNodes = new HashSet<>();
              i = processHostList(args, i, "include", includedNodes);
              b.setIncludedNodes(includedNodes);
            } else if ("-source".equalsIgnoreCase(args[i])) {
              Set<String> sourceNodes = new HashSet<>();
              i = processHostList(args, i, "source", sourceNodes);
              b.setSourceNodes(sourceNodes);
            } else if ("-blockpools".equalsIgnoreCase(args[i])) {
              checkArgument(
                  ++i < args.length,
                  "blockpools value is missing: args = "
                      + Arrays.toString(args));
              Set<String> blockpools = parseBlockPoolList(args[i]);
              LOG.info("DecommissionBalancer will run on the following blockpools: "
                  + blockpools);
              b.setBlockpools(blockpools);
            } else if ("-idleiterations".equalsIgnoreCase(args[i])) {
              checkArgument(++i < args.length,
                  "idleiterations value is missing: args = " + Arrays
                      .toString(args));
              int maxIdleIteration = Integer.parseInt(args[i]);
              LOG.info("Using a idleiterations of " + maxIdleIteration);
              b.setMaxIdleIteration(maxIdleIteration);
            } else if ("-runDuringUpgrade".equalsIgnoreCase(args[i])) {
              b.setRunDuringUpgrade(true);
              LOG.info("Will run the decommission balancer even during an ongoing HDFS "
                  + "upgrade. Most users will not want to run the decommission balancer "
                  + "during an upgrade since it will not affect used space "
                  + "on over-utilized machines.");
            } else if ("-dataCenterConstraint".equalsIgnoreCase(args[i])) {
              checkArgument(++i < args.length,
                  "Data center constraint is missing: args = " +
                      Arrays.toString(args));
              try {
                b.setDataCenterConstraint(args[i]);
                isSetDataCenter = true;
              } catch(IllegalArgumentException e) {
                System.err.println("Illegal data center constraint: " +
                    args[i]);
                throw e;
              }
            } else if ("-targetDataCenter".equalsIgnoreCase(args[i])) {
              checkArgument(++i < args.length,
                  "Target data center is missing: args = " +
                      Arrays.toString(args));
              try {
                b.setTargeDataCenter(args[i]);
              } catch(IllegalArgumentException e) {
                System.err.println("Illegal target data center: " +
                    args[i]);
                throw e;
              }
            } else if ("-asService".equalsIgnoreCase(args[i])) {
              b.setRunAsService(true);
              LOG.info("DecommmissionBalancer will run as a long running service");
            } else {
              throw new IllegalArgumentException("args = "
                  + Arrays.toString(args));
            }
          }
          if (checkDataCenter) {
            checkArgument(isSetDataCenter,
                "Please set the data center constraint for decommission balancer.");
          }
          checkArgument(excludedNodes == null || includedNodes == null,
              "-exclude and -include options cannot be specified together.");
        } catch(RuntimeException e) {
          printUsage(System.err);
          throw e;
        }
      }
      return b.build();
    }

    private static int processHostList(String[] args, int i, String type,
        Set<String> nodes) {
      Preconditions.checkArgument(++i < args.length,
          "List of %s nodes | -f <filename> is missing: args=%s",
          type, Arrays.toString(args));
      if ("-f".equalsIgnoreCase(args[i])) {
        Preconditions.checkArgument(++i < args.length,
            "File containing %s nodes is not specified: args=%s",
            type, Arrays.toString(args));

        final String filename = args[i];
        try {
          HostsFileReader.readFileToSet(type, filename, nodes);
        } catch (IOException e) {
          throw new IllegalArgumentException(
              "Failed to read " + type + " node list from file: " + filename);
        }
      } else {
        final String[] addresses = StringUtils.getTrimmedStrings(args[i]);
        nodes.addAll(Arrays.asList(addresses));
      }
      return i;
    }

    private static Set<String> parseBlockPoolList(String string) {
      String[] addrs = StringUtils.getTrimmedStrings(string);
      return new HashSet<String>(Arrays.asList(addrs));
    }

    private static void printUsage(PrintStream out) {
      out.println(USAGE + "\n");
    }
  }

  /**
   * Run a decommission balancer
   * @param args Command line arguments
   */
  public static void main(String[] args) {
    if (DFSUtil.parseHelpArgument(args, USAGE, System.out, true)) {
      System.exit(0);
    }

    try {
      System.exit(ToolRunner.run(new HdfsConfiguration(), new DecommissionBalancer.Cli(), args));
    } catch (Throwable e) {
      LOG.error("Exiting decommission balancer due an exception", e);
      System.exit(-1);
    }
  }
}
