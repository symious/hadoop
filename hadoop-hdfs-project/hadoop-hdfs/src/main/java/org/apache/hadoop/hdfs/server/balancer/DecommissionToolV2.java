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
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.BlockWithLocations;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

public class DecommissionToolV2 {
  private static final Logger LOG = LoggerFactory.getLogger(DecommissionToolV2.class);
  static final Path DECOMMISSION_ID_PATH = new Path("/system/decommission.id");

  private final NameNodeConnector nnc;
  private final String nsId;
  private final ExecutorService dispatcherServices;

  private final Map<DatanodeInfo, Map<Block, ReplicaDispatcher.ReplicaMoveTask>> movedBlocks
      = new ConcurrentHashMap<>();
  private final long getBlocksMinBlockSize;
  private final long getBlocksSize;
  private final int retryTimeout;
  private final Set<String> decommissionedDNs = new HashSet<>();

  private final Configuration conf;
  private final ReplicaDispatcher replicaDispatcher;

  public DecommissionToolV2(NameNodeConnector nnc1, Configuration conf)
      throws IOException {
    this.conf = conf;
    this.nnc = nnc1;
    this.nsId = nnc.getNameNodeUri().getAuthority();

    this.getBlocksMinBlockSize = 0;
    this.getBlocksSize = Balancer.getLongBytes(conf,
        DFSConfigKeys.DFS_BALANCER_GETBLOCKS_SIZE_KEY,
        DFSConfigKeys.DFS_BALANCER_GETBLOCKS_SIZE_DEFAULT);

    ThreadFactory tf = new ThreadFactoryBuilder()
        .setNameFormat(this.nsId + " Dispatcher Executor #%d")
        .build();
    this.dispatcherServices = HadoopExecutors.newFixedThreadPool(1000, tf);

    this.retryTimeout = conf.getInt(DFSConfigKeys.DFS_DECOMMISSION_RETRY_TIMEOUT_KEY,
        DFSConfigKeys.DFS_DECOMMISSION_RETRY_TIMEOUT_DEFAULT);

    this.replicaDispatcher = new ReplicaDispatcher(nnc1, conf);
  }

  public void shutdown() {
    if (this.replicaDispatcher != null) {
      this.replicaDispatcher.shutdown();
    }

    if (this.dispatcherServices != null) {
      this.dispatcherServices.shutdown();
      this.dispatcherServices.shutdownNow();
    }

    this.movedBlocks.clear();
    this.decommissionedDNs.clear();
  }

  public void start() throws IOException {
    this.replicaDispatcher.start();
  }

  public boolean decommissionDataNodes() throws Exception {
    try {
      int loopCount = 5;
      while (loopCount-- > 0) {
        List<DatanodeInfo> decommissioningDNs = nnc.getLiveAndDecommissionDatanodeStorageReport();
        if (decommissioningDNs != null && !decommissioningDNs.isEmpty()) {
          List<Future<Boolean>> futures = new ArrayList<>();
          for (DatanodeInfo dn : decommissioningDNs) {
            if (!decommissionedDNs.contains(dn.getDatanodeUuid())) {
              loopCount = 5;
              LOG.info("[{}] {} {} need to be decommissioned.", nnc.getBlockpoolID(), this.nsId, dn);
              futures.add(this.dispatcherServices.submit(() -> {
                String threadName = this.nsId + "_" + dn.getXferAddr();
                Thread.currentThread().setName(threadName);
                return dispatchDN(dn);
              }));
            }
          }
          if (!futures.isEmpty()) {
            for (Future<Boolean> f : futures) {
              f.get();
            }
          }
        }
        Thread.sleep(1000 * 20);
        LOG.info("[{}] There is no datanode need to be decommissioned, loopCount is {}.",
            this.nsId, loopCount);
      }
      return true;
    } finally {
      shutdown();
    }
  }

  private boolean dispatchDN(DatanodeInfo dn) throws IOException, InterruptedException {
    long maxCheckTimes = this.conf.getInt(DFSConfigKeys.DFS_DECOMMISSION_MAX_CHECK_TIMES_KEY,
        DFSConfigKeys.DFS_DECOMMISSION_MAX_CHECK_TIMES_DEFAULT);
    long loopCounter = maxCheckTimes;
    while (true) {
      BlocksWithLocations blocksWithLocations = this.nnc.getBlocks(
          dn, this.getBlocksSize, this.getBlocksMinBlockSize);
      BlockWithLocations[] blocks = blocksWithLocations.getBlocks();
      boolean canExit = true;
      for (BlockWithLocations blockWithLocation : blocks) {
        Block block = blockWithLocation.getBlock();
        Map<Block, ReplicaDispatcher.ReplicaMoveTask> tasks = this.movedBlocks.computeIfAbsent(
            dn, k -> new ConcurrentHashMap<>());
        ReplicaDispatcher.ReplicaMoveTask task = tasks.get(block);
        if (task == null || task.canRetry(this.retryTimeout)) {
          loopCounter = maxCheckTimes;
          canExit = false;
          try {
            ReplicaDispatcher.ReplicaMoveTask movingTask = this.replicaDispatcher
                .dispatchBlockWithLocations(dn, blockWithLocation);
            tasks.put(block, movingTask);
          } catch (Throwable e) {
            LOG.warn("Failed to build the task for {} with {} in {} by {}.",
                block, blockWithLocation, dn, this.nsId, e);
          }
        } else if (task.getEndTime() == 0) { // means: moving
          loopCounter = maxCheckTimes;
          canExit = false;
        }
      }

      if (canExit) {
        loopCounter -= 1;
        LOG.info("[{}] {} DN {} can be decommissioned in {} loop, {} blocks are remained",
            nnc.getBlockpoolID(), this.nsId, dn, loopCounter,
            blocks.length);
        if (loopCounter <= 0) {
          LOG.info("[{}] {} DN {} still has {} blocks need to be decommissioned, the blocks " +
                  "are {}.", nnc.getBlockpoolID(), this.nsId, dn,
              blocks.length, Arrays.asList(blocks));
          break;
        } else {
          Thread.sleep(1000 * 60);
        }
      }
    }
    movedBlocks.remove(dn);
    decommissionedDNs.add(dn.getDatanodeUuid());
    return true;
  }

  static class Cli extends Configured implements Tool {
    /**
     * Parse arguments and then run Decommission Tool.
     *
     * @param args command specific arguments.
     * @return exit code. 0 indicates success, non-zero indicates failure.
     */
    @Override
    public int run(String[] args) {
      final long startTime = Time.monotonicNow();
      final Configuration conf = getConf();
      try {
        Collection<String> namespaces = new ArrayList<>(conf.getStringCollection("namespaces"));
        if (namespaces.isEmpty()) {
          namespaces = DFSUtilClient.getNameServiceIds(conf);
        }
        final Collection<String> nsIds = namespaces;
        final Collection<URI> namenodes = new ArrayList<>();
        for (String ns : nsIds) {
          namenodes.add(DFSUtil.createUri(HdfsConstants.HDFS_URI_SCHEME, ns, -1));
        }
        return doMigration(namenodes, nsIds, conf);
      } catch (Exception e) {
        System.out.println(e + ".  Exiting ...");
        return ExitStatus.IO_EXCEPTION.getExitCode();
      } finally {
        System.out.format("%-24s ", DateFormat.getDateTimeInstance().format(new Date()));
        LOG.info("Decommission migrating took {}.",
            Balancer.time2Str(Time.monotonicNow() - startTime));
      }
    }
  }

  private static Path getPathForDecommission() {
    return new Path(DECOMMISSION_ID_PATH, "decommission-" + Time.now());
  }

  private static int doMigration(Collection<URI> namenodes,
      Collection<String> nsIds, Configuration conf) throws Exception {
    boolean checkAllNNs = conf.getBoolean(
        DFSConfigKeys.DFS_DECOMMISSION_BALANCER_CHECK_ALL_NAMENODE_KEY,
        DFSConfigKeys.DFS_DECOMMISSION_BALANCER_CHECK_ALL_NAMENODE_DEFAULT);

    LOG.info("namenodes = {}, checkAllNNs = {}.", namenodes, checkAllNNs);

    List<NameNodeConnector> connectors = new ArrayList<>();
    try {
      if (checkAllNNs) {
        for (URI nn : namenodes) {
          Map<String, InetSocketAddress> addresses =
              DFSUtilClient.getHaNnRpcAddresses(conf).get(nn.getAuthority());
          for (InetSocketAddress address : addresses.values()) {
            LOG.debug("Add the address: {} into the address list", address.toString());
            NameNodeConnector specificnnc = new NameNodeConnector(
                DecommissionTool.class.getSimpleName(), nn, address,
                nn.getAuthority(), getPathForDecommission(),
                null, conf, 5);
            connectors.add(specificnnc);
          }
        }
      } else {
        connectors = NameNodeConnector.newNameNodeConnectors(namenodes, nsIds,
            DecommissionTool.class.getSimpleName(), getPathForDecommission(), conf, 5);
      }

      LOG.info("Namenode list is {}", connectors);

      ThreadFactory tf = new ThreadFactoryBuilder()
          .setNameFormat("Namespace Executor #%d")
          .build();
      ExecutorService executorService = HadoopExecutors.newFixedThreadPool(connectors.size(), tf);

      Collections.shuffle(connectors);
      List<Future<Boolean>> futures = new ArrayList<>();
      for (NameNodeConnector nnc : connectors) {
        futures.add(executorService.submit(() -> {
          DecommissionToolV2 d2 = null;
          try {
            d2 = new DecommissionToolV2(nnc, conf);
            d2.start();
            return d2.decommissionDataNodes();
          } catch (Throwable e) {
            if (d2 != null) {
              d2.shutdown();
            }
            throw  e;
          }
        }));
      }
      for (Future<Boolean> f : futures) {
        f.get();
      }
    } finally {
      for (NameNodeConnector nnc : connectors) {
        IOUtils.cleanupWithLogger(LOG, nnc);
      }
    }
    return ExitStatus.SUCCESS.getExitCode();
  }

  /**
   * Run a decommission tool
   * @param args Command line arguments
   */
  public static void main(String[] args) {
    try {
      System.exit(ToolRunner.run(new HdfsConfiguration(), new DecommissionToolV2.Cli(), args));
    } catch (Throwable e) {
      LOG.error("Exiting decommission tool due an exception", e);
      System.exit(-1);
    }
  }
}
