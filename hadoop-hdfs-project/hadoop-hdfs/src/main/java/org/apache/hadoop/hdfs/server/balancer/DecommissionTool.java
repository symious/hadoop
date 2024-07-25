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
import org.apache.hadoop.hdfs.net.NetworkTopologyUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.datatransfer.TrustedChannelResolver;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataTransferSaslUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferClient;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicy;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.BlockWithLocations;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.StripedBlockWithLocations;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.hdfs.protocolPB.PBHelperClient.vintPrefixed;
import static org.apache.hadoop.hdfs.util.StripedBlockUtil.getInternalBlockLength;


public class DecommissionTool {
  private static final Logger LOG = LoggerFactory.getLogger(DecommissionTool.class);
  static final Path DECOMMISSION_ID_PATH = new Path("/system/decommission.id");


  private final boolean enableCrossDC;
  private final DataTransferThrottler crossDCThrottler;
  private final NameNodeConnector nnc;
  private final String nsId;
  private final ExecutorService dispatcherServices;

  private final Map<DatanodeInfo, Map<Block, Task>> movedBlocks = new ConcurrentHashMap<>();
  private final long getBlocksMinBlockSize;
  private final long getBlocksSize;

  private final BlockPlacementPolicyForDecommissionTool blockPlacement;
  private final String targetDC;
  private final SaslDataTransferClient saslClient;
  private final int blockMoveTimeout;
  private final int retryTimeout;
  private final MoverManager moverManager;
  private final Set<String> decommissionedDNs = new HashSet<>();
  private final Configuration conf;

  public DecommissionTool(NameNodeConnector nnc1, Configuration conf, MoverManager moverManager) {
    this.conf = conf;
    this.nnc = nnc1;
    this.nsId = nnc.getNameNodeUri().getAuthority();
    this.moverManager = moverManager;
    this.enableCrossDC = conf.getBoolean(DFSConfigKeys.DFS_DECOMMISSION_ENABLE_CROSS_DC_KEY,
        DFSConfigKeys.DFS_DECOMMISSION_ENABLE_CROSS_DC_DEFAULT);
    long bandwidth = conf.getLong(DFSConfigKeys.DFS_DECOMMISSION_CROSS_DC_BANDWIDTH_KEY,
        DFSConfigKeys.DFS_DECOMMISSION_CROSS_DC_BANDWIDTH_DEFAULT);
    if (bandwidth > 0) {
      this.crossDCThrottler = new DataTransferThrottler(bandwidth);
    } else {
      this.crossDCThrottler = null;
    }
    this.getBlocksMinBlockSize = 0;
    this.getBlocksSize = Balancer.getLongBytes(conf,
        DFSConfigKeys.DFS_BALANCER_GETBLOCKS_SIZE_KEY,
        DFSConfigKeys.DFS_BALANCER_GETBLOCKS_SIZE_DEFAULT);

    this.targetDC = conf.get(DFSConfigKeys.DFS_DECOMMISSION_TARGET_DC_KEY, null);

    NetworkTopology clusterMap = NetworkTopology.getInstance(conf);
    boolean enableUpgradeDomain = conf.getClass(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY,
        DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_DEFAULT,
        BlockPlacementPolicy.class).getSimpleName().contains("UpgradeDomain");
    this.blockPlacement = new BlockPlacementPolicyForDecommissionTool(
        clusterMap, this.nsId, enableUpgradeDomain);

    this.saslClient = new SaslDataTransferClient(conf,
        DataTransferSaslUtil.getSaslPropertiesResolver(conf),
        TrustedChannelResolver.getInstance(conf), nnc.fallbackToSimpleAuth);

    ThreadFactory tf = new ThreadFactoryBuilder()
        .setNameFormat(this.nsId + " Dispatcher Executor #%d")
        .build();
    this.dispatcherServices = HadoopExecutors.newFixedThreadPool(1000, tf);

    this.blockMoveTimeout = conf.getInt(DFSConfigKeys.DFS_BALANCER_BLOCK_MOVE_TIMEOUT,
        DFSConfigKeys.DFS_BALANCER_BLOCK_MOVE_TIMEOUT_DEFAULT);

    this.retryTimeout = conf.getInt(DFSConfigKeys.DFS_DECOMMISSION_RETRY_TIMEOUT_KEY,
        DFSConfigKeys.DFS_DECOMMISSION_RETRY_TIMEOUT_DEFAULT);

    LOG.info("nnc {}, enableCrossDC {}, bandwidth {}, getBlocksSize {}, targetDC {}," +
        " blockMoveTimeout {}, retryTimeout {}.", this.nnc, this.enableCrossDC,
        bandwidth, this.getBlocksSize, this.targetDC, this.blockMoveTimeout,
        this.retryTimeout);
  }

  public void shutdown() {
    dispatcherServices.shutdown();
    dispatcherServices.shutdownNow();

    this.movedBlocks.clear();
    this.decommissionedDNs.clear();
  }

  public void init() throws IOException {
    final List<DatanodeStorageReport> reports = Arrays.asList(nnc.getLiveDatanodeStorageReport());
    Collections.shuffle(reports);
    for (DatanodeStorageReport r : reports) {
      final DatanodeInfo datanode = r.getDatanodeInfo();
      LOG.debug("[{}] DataNode {} state {}.", this.nsId,
          datanode, datanode.getAdminState());
      this.blockPlacement.addNode(datanode);
    }
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
        Map<Block, Task> tasks = this.movedBlocks.computeIfAbsent(
            dn, k -> new ConcurrentHashMap<>());
        Task task = tasks.get(block);
        if (task == null || task.canRetry(retryTimeout)) {
          loopCounter = maxCheckTimes;
          canExit = false;
          try {
            Task movingTask = new Task(dn, blockWithLocation);
            movingTask.chooseTarget();
            movingTask.chooseProxy();
            tasks.put(block, movingTask);
            this.moverManager.addTask(movingTask);
          } catch (Throwable e) {
            LOG.warn("Failed to build the task for {} with {} in {} by {}.",
                block, blockWithLocation, dn, this.nsId, e);
          }
        } else if (task.endTime == 0) { // means: moving
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

  public enum State {
    PENDING,
    RUNNING,
    SUCCESS,
    FAILED
  }

  public class Task {
    private final DatanodeInfo source;
    private DatanodeInfo target;
    private DatanodeInfo proxy;
    private final Block block;
    private final BlockWithLocations blockWithLocations;
    private final List<DatanodeInfo> locations;
    private volatile long endTime = 0;
    private volatile State state;

    public Task(DatanodeInfo source, BlockWithLocations blockWithLocations) {
      this.source = source;
      this.blockWithLocations = blockWithLocations;
      this.locations = new ArrayList<>();
      for (String uuId : this.blockWithLocations.getDatanodeUuids()) {
        DatanodeInfo dn = blockPlacement.getDataNode(uuId);
        if (dn == null) {
          if (this.blockWithLocations instanceof StripedBlockWithLocations) {
            this.locations.add(null);
          } else {
            LOG.warn("[{}] {} cannot find datanode for {} with uuId {}.",
                nnc.getBlockpoolID(), nsId, blockWithLocations.getBlock(), uuId);
          }
        } else {
          this.locations.add(dn);
        }
      }
      if (this.blockWithLocations instanceof StripedBlockWithLocations) {
        StripedBlockWithLocations stripedBlockWithLocations =
            (StripedBlockWithLocations) this.blockWithLocations;

        byte[] indices = stripedBlockWithLocations.getIndices();
        Block globalBlock = stripedBlockWithLocations.getBlock();
        int cellSize = stripedBlockWithLocations.getCellSize();
        short dataBlockNum = stripedBlockWithLocations.getDataBlockNum();

        int idxInLocs = this.locations.indexOf(this.source);
        byte idxInGroup = indices[idxInLocs];
        long blkId = globalBlock.getBlockId() + idxInGroup;
        long numBytes = getInternalBlockLength(globalBlock.getNumBytes(), cellSize,
            dataBlockNum, idxInGroup);
        LOG.debug("[{}] global block {} source {} locations {} idxInLocs {} idxInGroup {}" +
                " blkId {} numBytes {}.", nnc.getBlockpoolID(), globalBlock.getBlockId(),
            this.source, idxInLocs, this.locations, idxInGroup, blkId, numBytes);
        Block blk = new Block(globalBlock);
        blk.setBlockId(blkId);
        blk.setNumBytes(numBytes);
        this.block = blk;
      } else {
        this.block = this.blockWithLocations.getBlock();
      }
      this.locations.add(this.source);
      state = State.PENDING;
    }

    public boolean canRetry(long timeout) {
      return this.state == State.FAILED && timeout > 0 && (Time.monotonicNow() - endTime > timeout);
    }

    public Block getBlock() {
      return this.blockWithLocations.getBlock();
    }

    public void chooseTarget() throws IOException {
      DatanodeInfo targetDN;
      if (this.blockWithLocations instanceof StripedBlockWithLocations) {
        targetDN = blockPlacement.chooseTargetForStripeBlock(
            this.block, this.source, this.target, this.locations, targetDC);
      } else {
        targetDN = blockPlacement.chooseTargetForContiguousBlock(
            this.block, this.source, this.target, this.locations, targetDC);
      }

      if (targetDN == null) {
        LOG.warn("[{}] Cannot choose target DN for {} with locations {} and source DN is {}.",
            nsId, this.block, this.locations, this.source);
        throw new IOException("Cannot choose target DN for " + this.block);
      }
      this.target = targetDN;
    }

    public void chooseProxy() {
      this.proxy = DecommissionTool.chooseProxy(this.blockWithLocations, this.source,
          this.target, this.proxy, this.locations, enableCrossDC, nsId);
    }

    public String toString() {
      String bStr = nsId + " " + this.block + " with size="
          + this.block.getNumBytes() + " ";
      return bStr + "from " + this.source.getXferAddr() + " to " + this.target
          .getXferAddr() + " through " + this.proxy.getXferAddr();
    }

    /** Dispatch the move to the proxy source & wait for the response. */
    public boolean dispatch(boolean firstDispatch) throws IOException {
      this.state = State.RUNNING;
      if (!isSameDC(this.target, this.proxy) && crossDCThrottler != null) {
        // here means this replace block operations may cross dc.
        LOG.debug("[{}][Throttle] Acquire {}.", nsId, this.block.getNumBytes());
        crossDCThrottler.throttle(this.block.getNumBytes());
      }
      Socket sock = new Socket();
      DataOutputStream out = null;
      DataInputStream in = null;
      try {
        LOG.info("Start moving " + this);

        sock.connect(NetUtils.createSocketAddr(this.target.getXferAddr()),
            HdfsConstants.READ_TIMEOUT);

        // Set read timeout so that it doesn't hang forever against
        // unresponsive nodes. Datanode normally sends IN_PROGRESS response
        // twice within the client read timeout period (every 30 seconds by
        // default). Here, we make it give up after 5 minutes of no response.
        sock.setSoTimeout(HdfsConstants.READ_TIMEOUT * 5);
        sock.setKeepAlive(true);

        OutputStream unbufOut = sock.getOutputStream();
        InputStream unbufIn = sock.getInputStream();
        ExtendedBlock eb = new ExtendedBlock(nnc.getBlockpoolID(), this.block);
        final KeyManager km = nnc.getKeyManager();
        Token<BlockTokenIdentifier> accessToken = km.getAccessToken(eb,
            new StorageType[]{StorageType.DISK}, new String[0]);
        IOStreamPair saslStreams = saslClient.socketSend(sock, unbufOut, unbufIn, km,
            accessToken, this.target);
        unbufOut = saslStreams.out;
        unbufIn = saslStreams.in;
        out = new DataOutputStream(new BufferedOutputStream(unbufOut, 4096));
        in = new DataInputStream(new BufferedInputStream(unbufIn, 4096));

        new Sender(out).replaceBlock(eb, StorageType.DISK, accessToken,
            this.source.getDatanodeUuid(), this.proxy, null);
        receiveResponse(in);
        nnc.addBytesMoved(this.block.getNumBytes());
        LOG.info("Successfully moved " + this);
        this.endTime = Time.monotonicNow();
        this.state = State.SUCCESS;
      } catch (IOException e) {
        if (firstDispatch) {
          this.state = State.PENDING;
          return true;
        } else {
          this.endTime = Time.monotonicNow();
          this.state = State.FAILED;
          throw e;
        }
      } finally {
        IOUtils.closeStream(out);
        IOUtils.closeStream(in);
        IOUtils.closeSocket(sock);
      }

      return false;
    }

    public void callBack() {
      /*Map<Block, Task> tasks = movedBlocks.get(this.source);
      if (tasks != null) {
        tasks.remove(getBlock());
      }*/
      // do nothing.
    }

    /** Receive a reportedBlock copy response from the input stream */
    protected void receiveResponse(DataInputStream in) throws IOException {
      long startTime = Time.monotonicNow();
      BlockOpResponseProto response = BlockOpResponseProto.parseFrom(vintPrefixed(in));
      while (response.getStatus() == Status.IN_PROGRESS) {
        // read intermediate responses
        response = BlockOpResponseProto.parseFrom(vintPrefixed(in));

        if (blockMoveTimeout > 0 && (Time.monotonicNow() - startTime > blockMoveTimeout)) {
          throw new IOException("Block " + this.block + " move timed out");
        }
      }
      String logInfo = "reportedBlock move is failed";
      DataTransferProtoUtil.checkBlockOpStatus(response, logInfo, true);
    }
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

  public static boolean isSameDC(DatanodeInfo dn1, DatanodeInfo dn2) {
    String dc1 = NetworkTopologyUtil.getDataCenter(dn1);
    String dc2 = NetworkTopologyUtil.getDataCenter(dn2);
    return dc1.equalsIgnoreCase(dc2);
  }

  public static boolean isSameRack(DatanodeInfo dn1, DatanodeInfo dn2) {
    String rack1 = dn1.getNetworkLocation();
    String rack2 = dn2.getNetworkLocation();
    return rack1.equalsIgnoreCase(rack2);
  }

  public static DatanodeInfo chooseProxy(BlockWithLocations blockWithLocations,
      DatanodeInfo source, DatanodeInfo target, DatanodeInfo oldProxy,
      List<DatanodeInfo> locations, boolean enableCrossDC, String namespace) {
    Block block = blockWithLocations.getBlock();
    if (blockWithLocations instanceof StripedBlockWithLocations) {
      LOG.debug("[{}] [StripBlock] block {} sourceNode {} targetNode {} proxyNode {}.",
          namespace, block, source, target, source);
      return source;
    }

    LOG.debug("[{}] Block {} sourceNode {} locations {} target {}.",
        namespace, block, source, locations, target);
    if (locations.size() == 1) {
      return locations.get(0);
    }
    // Split locations by DC
    List<DatanodeInfo> replicasInOtherDC = new ArrayList<>();
    List<DatanodeInfo> otherRackLocs = new ArrayList<>();
    List<DatanodeInfo> sameRackLocs = new ArrayList<>();

    // Same rack.
    for (DatanodeInfo dn : locations) {
      if (!source.getDatanodeUuid().equals(dn.getDatanodeUuid()) &&
          (oldProxy == null || !oldProxy.getDatanodeUuid().equals(dn.getDatanodeUuid()))) {
        // Tips: almost all blocks will return here.
        if (isSameRack(dn, target)) {
          LOG.debug("[{}] [SameRack] block {} target {}, proxy {}.", namespace, block, target, dn);
          return dn;
        }
        if (isSameDC(source, dn)) {
          if (isSameRack(source, dn)) {
            sameRackLocs.add(dn);
          } else {
            otherRackLocs.add(dn);
          }
        } else {
          replicasInOtherDC.add(dn);
        }
      }
    }
    if (sameRackLocs.size() > 1) {
      Collections.shuffle(sameRackLocs);
    }
    if (otherRackLocs.size() > 1) {
      Collections.shuffle(otherRackLocs);
    }
    if (replicasInOtherDC.size() > 1) {
      Collections.shuffle(replicasInOtherDC);
    }
    LOG.debug("[{}] Block {} source {} target {}, sameRackLocs {}, otherRackLocs {}, " +
            "replicasInOtherDC {}, enableCrossDC is {}", namespace, block, source, target,
        sameRackLocs, otherRackLocs, replicasInOtherDC, enableCrossDC);

    DatanodeInfo proxy;
    if (otherRackLocs.size() > 0) {
      proxy = otherRackLocs.get(0);
      LOG.debug("[{}] [OtherRacks] block {} target {}, proxy {}.", namespace, block, target, proxy);
      return proxy;
    }

    if (sameRackLocs.size() > 0) {
      proxy = sameRackLocs.get(0);
      LOG.debug("[{}] [PreferSameRack] block {} target {}, proxy {}.", namespace, block, target, proxy);
      return proxy;
    }

    if (enableCrossDC && replicasInOtherDC.size() > 0) {
      proxy = replicasInOtherDC.get(0);
      LOG.debug("[{}] [OtherDC] block {} target {}, proxy {}.", namespace, block, target, proxy);
      return proxy;
    }

    proxy = source;
    LOG.debug("[{}] [preferSource] block {} target {}, proxy {}.", namespace, block, target, source);
    return proxy;
  }

  static class MoverManager {
    private final BlockingQueue<Task> taskBlockingQueue;
    private final MoverThread[] movers;
    private final AtomicBoolean shouldRun = new AtomicBoolean(true);

    MoverManager(Configuration conf) {
      int queueSize = conf.getInt(DFSConfigKeys.DFS_DECOMMISSION_QUEUE_SIZE_KEY,
          DFSConfigKeys.DFS_DECOMMISSION_QUEUE_SIZE_DEFAULT);
      this.taskBlockingQueue = new LinkedBlockingQueue<>(queueSize);

      int handlerCount = conf.getInt(DFSConfigKeys.DFS_DECOMMISSION_HANDLER_COUNT_KEY,
          DFSConfigKeys.DFS_DECOMMISSION_HANDLER_COUNT_DEFAULT);
      this.movers = new MoverThread[handlerCount];
      for (int i = 0; i < handlerCount; i++) {
        movers[i] = new MoverThread(this.taskBlockingQueue, i, shouldRun);
        movers[i].start();
      }
    }

    public void addTask(Task task) throws InterruptedException {
      this.taskBlockingQueue.put(task);
    }

    public void shutdown() {
      this.shouldRun.compareAndSet(true, false);
      if (this.movers != null) {
        for (MoverThread mover : movers) {
          if (mover != null) {
            mover.interrupt();
          }
        }
      }
      this.taskBlockingQueue.clear();
    }
  }

  static class MoverThread extends Thread {
    private final BlockingQueue<Task> movingBlocks;
    private final AtomicBoolean shouldRun;
    MoverThread(BlockingQueue<Task> movingBlocks, int instanceNumber, AtomicBoolean shouldRun) {
      this.movingBlocks = movingBlocks;
      this.shouldRun = shouldRun;
      this.setName("Mover thread " + instanceNumber);
    }

    public void run() {
      while (shouldRun.get()) {
        Task task = null;
        try {
          task = this.movingBlocks.take();
          boolean shouldRetry = task.dispatch(true);
          if (shouldRetry) {
            LOG.info("Will retry move " + task);
            task.chooseTarget();
            task.chooseProxy();
            task.dispatch(false);
          }
        } catch (InterruptedException e) {
          // ignore
        } catch (Exception e) {
          LOG.warn("Failed move {} with error message {}.", task, e.getMessage());
          if (task != null) {
            task.callBack();
          }
        }
      }
    }
  }

  private static int doMigration(Collection<URI> namenodes,
      Collection<String> nsIds, Configuration conf) throws Exception {
    boolean checkAllNNs = conf.getBoolean(
        DFSConfigKeys.DFS_DECOMMISSION_BALANCER_CHECK_ALL_NAMENODE_KEY,
        DFSConfigKeys.DFS_DECOMMISSION_BALANCER_CHECK_ALL_NAMENODE_DEFAULT);

    LOG.info("namenodes = {}, checkAllNNs = {}.", namenodes, checkAllNNs);

    List<NameNodeConnector> connectors = new ArrayList<>();
    MoverManager moverManager = null;
    try {
      if (checkAllNNs) {
        for (URI namenode : namenodes) {
          Map<String, InetSocketAddress> addresses =
              DFSUtilClient.getHaNnRpcAddresses(conf).get(namenode.getAuthority());
          for (InetSocketAddress address : addresses.values()) {
            LOG.debug("Add the address: {} into the address list", address.toString());
            NameNodeConnector specificnnc = new NameNodeConnector(
                DecommissionTool.class.getSimpleName(), namenode, address,
                namenode.getAuthority(), getPathForDecommission(),
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
      moverManager = new MoverManager(conf);

      Collections.shuffle(connectors);
      List<Future<Boolean>> futures = new ArrayList<>();
      for (NameNodeConnector nnc : connectors) {
        MoverManager finalMoverManager = moverManager;
        futures.add(executorService.submit(() -> {
          final DecommissionTool d2 = new DecommissionTool(nnc, conf, finalMoverManager);
          d2.init();
          return d2.decommissionDataNodes();
        }));
      }
      for (Future<Boolean> f : futures) {
        f.get();
      }
    } finally {
      for (NameNodeConnector nnc : connectors) {
        IOUtils.cleanupWithLogger(LOG, nnc);
      }
      if (moverManager != null) {
        moverManager.shutdown();
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
      System.exit(ToolRunner.run(new HdfsConfiguration(), new DecommissionTool.Cli(), args));
    } catch (Throwable e) {
      LOG.error("Exiting decommission tool due an exception", e);
      System.exit(-1);
    }
  }
}
