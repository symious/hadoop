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
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher.DDatanode.StorageGroup;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
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
import java.util.HashSet;
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

  private final List<Dispatcher.Source> sourceList = new LinkedList<>();
  private final List<StorageGroup> belowAvgUtilized = new LinkedList<>();
  private final List<StorageGroup> underUtilized = new LinkedList<>();
  private final List<StorageGroup> overUtilized = new LinkedList<>();
  private final List<StorageGroup> targetNodes = new LinkedList<>();
  private final long miniMaxSize2Move = 1024 * 1024 * 1024;

  // Source DC
  private String dataCenterConstraint = null;
  // Target DC
  private String targetDataCenter = null;
  static final Path DECOMMISSION_BALANCER_ID_PATH =
      new Path("/system/decommission_balancer.id");
  private final boolean supportCrossDC;
  private final DataTransferThrottler crossDCThrottler;

  /**
   * Construct a decommission balancer.
   * Initialize balancer and constraint the source DC and target DC inside decommission balancer
   */
  DecommissionBalancer(NameNodeConnector nnc, BalancerParameters p, Configuration conf,
      String dataCenterConstraint) {
    super(nnc, p, conf);
    this.policy = BalancingPolicy.Decommission.INSTANCE;
    this.dataCenterConstraint = dataCenterConstraint;
    this.targetDataCenter = p.getTargetDataCenter();
    this.supportCrossDC = conf.getBoolean(
        DFSConfigKeys.DFS_DECOMMISSION_BALANCER_ENABLE_CROSS_DC_KEY,
        DFSConfigKeys.DFS_DECOMMISSION_BALANCER_ENABLE_CROSS_DC_DEFAULT);
    long crossDCBandwidth = conf.getLong(
        DFSConfigKeys.DFS_DECOMMISSION_BALANCER_CROSS_DC_BANDWIDTH_KEY,
        DFSConfigKeys.DFS_DECOMMISSION_BALANCER_CROSS_DC_BANDWIDTH_DEFAULT);
    if (crossDCBandwidth > 0) {
      crossDCThrottler = new DataTransferThrottler(crossDCBandwidth);
    } else {
      crossDCThrottler = null;
    }
    if (supportCrossDC) {
      dispatcher.resetSupportCrossDC(true);
    }
    if (crossDCThrottler != null) {
      dispatcher.resetCrossDCThrottler(crossDCThrottler);
    }
    LOG.info("DecommissionBalancer, supportCrossDC {}, crossDCBandwidth {}.",
        supportCrossDC, crossDCBandwidth);
  }

  @Override
  protected long init(List<DatanodeStorageReport> reports) {
    long sizeToMove = 0L;
    // compute average utilization
    for (DatanodeStorageReport r : reports) {
      policy.accumulateSpaces(r);
    }
    policy.initAvgUtilization();
    for (DatanodeStorageReport r : reports) {
      final Dispatcher.DDatanode dn = dispatcher.newDatanode(r.getDatanodeInfo());
      boolean isDecommissioning = r.getDatanodeInfo().isDecommissionInProgress();
      LOG.debug("Report {} state {}.", r.getDatanodeInfo(), r.getDatanodeInfo().getAdminState());

      for (StorageType t : StorageType.getMovableTypes()) {
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
          long usedSize = getUsed(r, t);
          sizeToMove += usedSize;

          if (usedSize == 0) {
            LOG.info("{} does not store data on {} storage.", s.getDatanodeInfo(), t);
            continue;
          }

          if (!sourceList.contains(s)) {
            LOG.info("Add {} into source.", s.getDatanodeInfo());
            sourceList.add(s);
            dispatcher.getStorageGroupMap().put(s);
          }
          continue;
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
        final double utilizationDiff = utilization - average;
        final double thresholdDiff = Math.abs(utilizationDiff) - threshold;
        final long maxSize2Move = getRemaining(r, t);

        StorageGroup s = dn.addTarget(t, maxSize2Move);
        boolean canMarkAsTarget = true;
        if (this.supportCrossDC) {
          // target DC is null && support cross DC read
          if (targetDataCenter != null) {
            if (!Dispatcher.Util.isInDataCenter(targetDataCenter, dn.getDatanodeInfo())) {
              canMarkAsTarget = false;
            }
          } else if (!Dispatcher.Util.isInDataCenter(dataCenterConstraint, dn.getDatanodeInfo())) {
            canMarkAsTarget = false;
          }
        }
        if (canMarkAsTarget) {
          LOG.info("{} [{}] has utilization={}, average={}, maxSize2Move={}.",
              dn, t, utilization, average, maxSize2Move);
          if (utilization >= average) {
            overUtilized.add(s);
          } else if (thresholdDiff <= 0) {
            belowAvgUtilized.add(s);
          } else {
            underUtilized.add(s);
          }
          targetNodes.add(s);
        } else {
          LOG.info("{} is only used as proxy node.", dn);
        }
        dispatcher.getStorageGroupMap().put(s);
      }
    }
    return sizeToMove;
  }


  @Override
  protected long chooseStorageGroups() {
    // TODO: match nodes on the same node group if cluster is node group aware

    LOG.info("chooseStorageGroups for {}: decommission => underUtilized number is {}.",
        Matcher.SAME_RACK, underUtilized.size());
    Collections.shuffle(underUtilized);
    chooseStorageGroups(sourceList, underUtilized);

    LOG.info("chooseStorageGroups for {}: decommission => belowAvgUtilized number is {}.",
        Matcher.SAME_RACK, belowAvgUtilized.size());
    Collections.shuffle(belowAvgUtilized);
    chooseStorageGroups(sourceList, belowAvgUtilized);

    LOG.info("chooseStorageGroups for {}: decommission => overUtilized number is {}.",
        Matcher.ANY_OTHER, overUtilized.size());
    Collections.shuffle(overUtilized);
    chooseStorageGroups(sourceList, overUtilized);

    return dispatcher.bytesToMove();
  }

  /**
   * For each datanode, choose matching nodes from the candidates. Either the
   * datanodes or the candidates are source nodes with (utilization > Avg), and
   * the others are target nodes with (utilization < Avg).
   */
  void chooseStorageGroups(List<Dispatcher.Source> groups, List<StorageGroup> candidates) {
    for (Dispatcher.Source g : groups) {
      long sourceMaxSize2Move = g.getMaxSize2Move();
      long avgTargetMaxSize2Move = Math.max(miniMaxSize2Move,
          (sourceMaxSize2Move + 1) / targetNodes.size());
      for (StorageGroup c : candidates) {
        if (matchStorageGroups(c, g, Matcher.ANY_OTHER)) {
          matchSourceWithTargetToMove(g, c, avgTargetMaxSize2Move);
        }
      }
    }
  }

  protected void matchSourceWithTargetToMove(Dispatcher.Source source,
      StorageGroup target, long avgTargetSize) {
    long targetMoveSize = Math.min(target.availableSizeToMove(), avgTargetSize);
    if (targetMoveSize <= 0) {
      LOG.warn("{} cannot receive more data.", target);
      return;
    }

    long size = Math.min(source.availableSizeToMove(), targetMoveSize);
    final Dispatcher.Task task = new Dispatcher.Task(target, size);
    source.addTask(task);
    dispatcher.add(source, target);
    LOG.info("Decided to move {} bytes from {} to {}.", StringUtils.byteDesc(size),
        source.getDisplayName(), target.getDisplayName());
  }

  private int getDecommissioningNodeCount() throws IOException {
    List<DatanodeInfo> allDecommissioningNodes = dispatcher.getDecommissioningNode();
    int count = 0;
    for (DatanodeInfo dn : allDecommissioningNodes) {
      if (Dispatcher.Util.isInDataCenter(dataCenterConstraint, dn)) {
        count++;
      }
    }
    return count;
  }

  @Override
  Result runOneIteration() {
    try {
      int decommissioningNodesCount = getDecommissioningNodeCount();
      if (decommissioningNodesCount == 0) {
        LOG.info("There is no node in decommissioning!");
        return newResult(ExitStatus.SUCCESS, 0, 0);
      }
      final List<DatanodeStorageReport> reports = dispatcher.init(true);
      final long bytesLeftToMove = init(reports);
      LOG.info("Need to move {} byte data to decommission {} DNs to finish current decommission.",
          StringUtils.byteDesc(bytesLeftToMove), decommissioningNodesCount);

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
        LOG.info("Will move {} in this iteration for {}",
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
    final long sleeptime = conf.getTimeDuration(
        DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT,
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
      for (int iteration = 0; !done; iteration++) {
        done = true;
        Collections.shuffle(connectors);
        for (NameNodeConnector nnc : connectors) {
          if (p.getBlockPools().size() == 0 || p.getBlockPools().contains(nnc.getBlockpoolID())) {
            // Check every block regardless of its size
            conf.setLong(DFSConfigKeys.DFS_BALANCER_GETBLOCKS_MIN_BLOCK_SIZE_KEY, 1);
            // If target DC is set, release the DC constraint
            String dcConstraint = p.getDataCenterConstraint();
            if (p.getTargetDataCenter() != null) {
              conf.setBoolean(DFSConfigKeys.DFS_DECOMMISSION_BALANCER_ENABLE_CROSS_DC_KEY, true);
            }
            final DecommissionBalancer b = new DecommissionBalancer(nnc, p, conf, dcConstraint);
            final Result r = b.runOneIteration();
            r.print(iteration, nnc, System.out);

            // clean all lists
            b.resetData(conf);

            if (r.getExitStatus() == ExitStatus.SUCCESS) {
              LOG.debug("Decommission on NN {} is done!", nnc);
            } else {
              if (r.getExitStatus() == ExitStatus.IN_PROGRESS) {
                done = false;
              } else if (r.getExitStatus() == ExitStatus.NO_MOVE_BLOCK) {
                // no block can be moved but decommissioning hasn't done, try the previous match
                LOG.info("Clean all the history matcher, retry all the possibility");
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
      for (NameNodeConnector nnc : connectors) {
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
        BalancerParameters balancerParameters = parse(args);
        Collection<String> namespaces = new ArrayList<>(conf.getStringCollection("namespaces"));
        if (namespaces.isEmpty()) {
          namespaces = DFSUtilClient.getNameServiceIds(conf);
        }
        final Collection<String> nsIds = namespaces;
        final Collection<URI> namenodes = DFSUtil.getInternalNsRpcUris(conf, nsIds);
        return DecommissionBalancer.run(namenodes, nsIds, balancerParameters, conf);
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
