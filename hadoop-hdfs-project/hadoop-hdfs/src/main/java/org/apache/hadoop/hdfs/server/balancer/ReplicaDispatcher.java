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
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.net.NetworkTopologyUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfoWithStorage;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.datatransfer.TrustedChannelResolver;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataTransferSaslUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferClient;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicy;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.BlockWithLocations;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.StripedBlockWithLocations;
import org.apache.hadoop.hdfs.server.zoneservice.metrics.ZoneProgressTracker;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.hdfs.protocolPB.PBHelperClient.vintPrefixed;
import static org.apache.hadoop.hdfs.util.StripedBlockUtil.getInternalBlockLength;

/**
 * A class to dispatch moving replicas for a block.
 * This dispatcher builds ReplicaMoveTasks and executes tasks by MoverManager.
 */
public class ReplicaDispatcher {
  private final static Logger LOG = LoggerFactory.getLogger(ReplicaDispatcher.class);

  private final Configuration conf;
  private final NameNodeConnector nnc;
  private final boolean enableCrossDC;
  private final DataTransferThrottler crossDCThrottler;
  private final String preferDC;
  private final int blockMoveTimeout;

  private final SaslDataTransferClient saslClient;
  private final BlockPlacementPolicyForReplicaDispatcher blockPlacementPolicy;

  private static MoverManager MOVER_MANAGER = null;

  public ReplicaDispatcher(NameNodeConnector nnc, Configuration conf)
      throws IOException {
    this.nnc = nnc;
    this.conf = conf;
    this.enableCrossDC = conf.getBoolean(
        DFSConfigKeys.DFS_DECOMMISSION_ENABLE_CROSS_DC_KEY,
        DFSConfigKeys.DFS_DECOMMISSION_ENABLE_CROSS_DC_DEFAULT);
    long bandwidth = conf.getLong(
        DFSConfigKeys.DFS_DECOMMISSION_CROSS_DC_BANDWIDTH_KEY,
        DFSConfigKeys.DFS_DECOMMISSION_CROSS_DC_BANDWIDTH_DEFAULT);
    if (bandwidth > 0) {
      this.crossDCThrottler = new DataTransferThrottler(bandwidth);
    } else {
      this.crossDCThrottler = null;
    }
    this.preferDC = conf.get(DFSConfigKeys.DFS_DECOMMISSION_TARGET_DC_KEY, null);
    this.blockMoveTimeout = conf.getInt(DFSConfigKeys.DFS_BALANCER_BLOCK_MOVE_TIMEOUT,
        DFSConfigKeys.DFS_BALANCER_BLOCK_MOVE_TIMEOUT_DEFAULT);

    this.saslClient = new SaslDataTransferClient(conf,
        DataTransferSaslUtil.getSaslPropertiesResolver(conf),
        TrustedChannelResolver.getInstance(conf), nnc.fallbackToSimpleAuth);

    boolean enableUpgradeDomain = conf.getClass(
        DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY,
        DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_DEFAULT,
        BlockPlacementPolicy.class).getSimpleName().contains("UpgradeDomain");

    this.blockPlacementPolicy = new BlockPlacementPolicyForReplicaDispatcher(
        nnc, conf, enableUpgradeDomain);
  }

  /**
   * Build a ReplicaMoveTask for block with locations and source datanode,
   * and schedule the ReplicaMoveTask to move replica from source to target datanode.
   * Decommission Tool can use this interface to move replicas.
   */
  public ReplicaMoveTask dispatchLocatedBlock(String fullPath,
      DatanodeInfo sourceDN, LocatedBlock locatedBlock, String preferDC,
      ErasureCodingPolicy ecPolicy, List<Node> excludeNodes) throws IOException {
    Block block = locatedBlock.getBlock().getLocalBlock();
    boolean isECBlock = locatedBlock instanceof LocatedStripedBlock;

    List<DatanodeInfo> locations = new ArrayList<>();
    StorageType sourceDNStorageType = null;

    DatanodeInfoWithStorage[] locs = locatedBlock.getLocations();
    List<Integer> adjustList = new ArrayList<>();
    for (int i = 0; i < locs.length; i++) {
      final DatanodeInfo dn = this.blockPlacementPolicy.getDataNode(
          locs[i].getDatanodeUuid(), locs[i].getStorageType());
      // find the storage type of the source DN
      if (sourceDN.getDatanodeUuid().equals(locs[i].getDatanodeUuid())) {
        sourceDNStorageType = locs[i].getStorageType();
      }
      if (dn != null) {
        locations.add(dn);
      } else if (isECBlock) { // The DN may be skipped, the indices should be adjusted.
        // some datanode may not in storageGroupMap due to decommission operation
        // or balancer cli with "-exclude" parameter
        adjustList.add(i);
      }
    }

    // For StripeBlock, the block id should be changed to internal block.
    if (isECBlock) {
      block = adjustIndices(block, sourceDN,
          ((LocatedStripedBlock) locatedBlock).getBlockIndices(),
          adjustList, locations, ecPolicy.getCellSize(), ecPolicy.getNumDataUnits());
    }
    if (excludeNodes == null || excludeNodes.isEmpty()) {
      excludeNodes = new ArrayList<>(locations);
    }
    return buildMoveTask(fullPath, block, isECBlock, sourceDN, locations,
        sourceDNStorageType, preferDC == null ? this.preferDC : preferDC,
        excludeNodes);
  }

  /**
   * Build a ReplicaMoveTask for block with locations and source datanode,
   * and schedule the ReplicaMoveTask to move replica from source to target datanode.
   * Decommission Tool can use this interface to move replicas.
   */
  public ReplicaMoveTask dispatchBlockWithLocations(
      DatanodeInfo sourceDN, BlockWithLocations blkLocs) throws IOException {
    Block block = blkLocs.getBlock();
    boolean isECBlock = blkLocs instanceof StripedBlockWithLocations;

    List<DatanodeInfo> locations = new ArrayList<>();
    List<Integer> adjustList = new ArrayList<>();

    final String[] datanodeUuids = blkLocs.getDatanodeUuids();
    final StorageType[] storageTypes = blkLocs.getStorageTypes();

    StorageType sourceDNStorageType = null;

    for (int i = 0; i < datanodeUuids.length; i++) {
      final DatanodeInfo dn = this.blockPlacementPolicy.getDataNode(
          datanodeUuids[i], storageTypes[i]);
      // find the storage type of the source DN
      if (sourceDN.getDatanodeUuid().equals(datanodeUuids[i])) {
        sourceDNStorageType = storageTypes[i];
      }
      if (dn != null) {
        locations.add(dn);
      } else if (isECBlock) { // The DN may be skipped, the indices should be adjusted.
        // some datanode may not in storageGroupMap due to decommission operation
        // or balancer cli with "-exclude" parameter
        adjustList.add(i);
      }
    }

    // For StripeBlock, the block id should be changed to internal block.
    if (isECBlock) {
      StripedBlockWithLocations stripeBlkLocs = (StripedBlockWithLocations) blkLocs;
      byte[] indices = ((StripedBlockWithLocations) blkLocs).getIndices();

      block = adjustIndices(block, sourceDN, indices, adjustList, locations,
          stripeBlkLocs.getCellSize(), stripeBlkLocs.getDataBlockNum());
    }
    return buildMoveTask(null, block, isECBlock, sourceDN, locations,
        sourceDNStorageType, this.preferDC, new ArrayList<>(locations));
  }

  /**
   * Adjust indices for stripe block and return the internal block.
   */
  private Block adjustIndices(Block block, DatanodeInfo sourceDN, byte[] indices,
      List<Integer> adjustList, List<DatanodeInfo> locations, int cellSize, int numDataUnits)
      throws IOException {
    if (!adjustList.isEmpty()) {
      byte[] newIndices = new byte[indices.length - adjustList.size()];
      for (int i = 0, j = 0; i < indices.length; ++i) {
        if (!adjustList.contains(i)) {
          newIndices[j] = indices[i];
          ++j;
        }
      }
      indices = newIndices;
    }

    int idxInLocs = locations.indexOf(sourceDN);
    if (idxInLocs == -1) {
      throw new IOException("Cannot find the source datanode for " + block);
    }
    byte idxInGroup = indices[idxInLocs];
    long blkId = block.getBlockId() + idxInGroup;
    long numBytes = getInternalBlockLength(block.getNumBytes(),
        cellSize, numDataUnits, idxInGroup);
    Block blk = new Block(block);
    blk.setBlockId(blkId);
    blk.setNumBytes(numBytes);
    return blk;
  }

  /**
   * Build a ReplicaMove task and schedule it.
   * Caller can get running state from this ReplicaMoveTask.
   */
  private ReplicaMoveTask buildMoveTask(String fullPath, Block block,
      boolean isECBlock, DatanodeInfo sourceDN, List<DatanodeInfo> locations,
      StorageType sourceDNStorageType, String preferDC, List<Node> excludeNodes)
      throws IOException {
    if (fullPath != null) {
      ZoneProgressTracker.queueFile(fullPath);
    }

    ReplicaMoveTask movingTask = new ReplicaMoveTask(fullPath, block, isECBlock,
        sourceDN, locations, sourceDNStorageType, preferDC, excludeNodes);
    movingTask.chooseTarget();
    movingTask.chooseProxy();

    // The moving task will be executed async.
    MOVER_MANAGER.addTask(movingTask);
    return movingTask;
  }

  public void start() {
    initMoverManager(this.conf);
    if (MOVER_MANAGER != null) {
      MOVER_MANAGER.incrReference();
    }
  }

  public void waitForMoveCompletion() {
    while (MOVER_MANAGER != null && !MOVER_MANAGER.noTasks()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        break;
      }
    }
  }

  public void shutdown() {
    shutdownMoverManager();
  }

  private static synchronized void initMoverManager(Configuration conf) {
    if (MOVER_MANAGER == null) {
      MOVER_MANAGER = new MoverManager(conf);
    }
    MOVER_MANAGER.incrReference();
  }

  private static synchronized void shutdownMoverManager() {
    if (MOVER_MANAGER != null) {
      if (MOVER_MANAGER.descReference() == 0) {
        MOVER_MANAGER.shutdown();
        MOVER_MANAGER = null;
      }
    }
  }

  public enum ReplicaMoverTaskState {
    PENDING,
    RUNNING,
    SUCCESS,
    FAILED
  }

  /**
   * A class to move replica from one datanode to another.
   */
  public class ReplicaMoveTask {
    private final DatanodeInfo source;
    private DatanodeInfo target;
    private DatanodeInfo proxy;

    private final Block block;
    private final boolean ecBlock;
    private final List<DatanodeInfo> locations;
    private final StorageType storageType;
    private volatile long endTime = 0;
    private volatile ReplicaMoverTaskState taskState;
    private final String preferDC;
    private final List<Node> excludeNodes;
    private final String fullPath;

    public ReplicaMoveTask(String fullPath, Block block, boolean ecBlock,
        DatanodeInfo source, List<DatanodeInfo> locations, StorageType storageType,
        String preferDC, List<Node> excludeNodes) {
      this.fullPath = fullPath;
      this.block = block;
      this.ecBlock = ecBlock;
      this.source = source;
      this.locations = locations;
      this.storageType = storageType;
      this.taskState = ReplicaMoverTaskState.PENDING;
      this.preferDC = preferDC;
      this.excludeNodes = excludeNodes;
    }

    /**
     * Return true if caller can retry this task again.
     */
    public boolean canRetry(long timeout) {
      return this.taskState == ReplicaMoverTaskState.FAILED && timeout > 0 && (
          Time.monotonicNow() - endTime > timeout);
    }

    public long getEndTime() {
      return this.endTime;
    }

    public Block getBlock() {
      return this.block;
    }

    public String getFullPath() {
      return this.fullPath;
    }

    public void chooseTarget() throws IOException {
      DatanodeInfo targetDN;
      if (this.ecBlock) {
        targetDN = blockPlacementPolicy.chooseTargetForStripeBlock(
            this.block, this.source, this.storageType, this.target,
            this.locations, this.preferDC, this.excludeNodes);
      } else {
        targetDN = blockPlacementPolicy.chooseTargetForContiguousBlock(
            this.block, this.source, this.storageType, this.target,
            this.locations, this.preferDC, this.excludeNodes);
      }

      if (targetDN == null) {
        LOG.warn("[{}] Cannot choose target DN for {} with locations {} and source DN is {}.",
            nnc.getNsId(), this.block, this.locations, this.source);
        throw new IOException("Cannot choose target DN for " + this.block);
      }
      this.target = targetDN;
    }

    /**
     * Choose a proxy to migrate this replica.
     */
    public void chooseProxy() {
      this.proxy = chooseProxy(this.ecBlock, this.source, this.target,
          this.proxy, this.locations, enableCrossDC, nnc.getNsId());
    }

    /**
     * Choose a better proxy datanode.
     */
    private DatanodeInfo chooseProxy(boolean isStripeBlock, DatanodeInfo source,
        DatanodeInfo target, DatanodeInfo oldProxy, List<DatanodeInfo> locations,
        boolean enableCrossDC, String namespace) {
      if (isStripeBlock) {
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
            (oldProxy == null ||
                !oldProxy.getDatanodeUuid().equals(dn.getDatanodeUuid()))) {
          // Tips: almost all blocks will return here.
          if (isSameRack(dn, target)) {
            LOG.debug("[{}] [SameRack] block {} target {}, proxy {}.",
                namespace, block, target, dn);
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
              "replicasInOtherDC {}, enableCrossDC is {}", namespace,
          this.block, source, target, sameRackLocs, otherRackLocs,
          replicasInOtherDC, enableCrossDC);

      DatanodeInfo proxy;
      if (!otherRackLocs.isEmpty()) {
        proxy = otherRackLocs.get(0);
        LOG.debug("[{}] [OtherRacks] block {} target {}, proxy {}.",
            namespace, this.block, target, proxy);
        return proxy;
      }

      if (!sameRackLocs.isEmpty()) {
        proxy = sameRackLocs.get(0);
        LOG.debug("[{}] [PreferSameRack] block {} target {}, proxy {}.",
            namespace, this.block, target, proxy);
        return proxy;
      }

      if (enableCrossDC && !replicasInOtherDC.isEmpty()) {
        proxy = replicasInOtherDC.get(0);
        LOG.debug("[{}] [OtherDC] block {} target {}, proxy {}.",
            namespace, this.block, target, proxy);
        return proxy;
      }

      proxy = source;
      LOG.debug("[{}] [preferSource] block {} target {}, proxy {}.",
          namespace, this.block, target, source);
      return proxy;
    }

    public String toString() {
      String bStr = nnc.getNsId() + " " + this.block + " with size=" + this.block.getNumBytes();
      return bStr + " from " + this.source.getXferAddr() + " to " + this.target
          .getXferAddr() + " through " + this.proxy.getXferAddr();
    }

    /** Dispatch the move to the proxy source & wait for the response. */
    public boolean dispatch(boolean firstDispatch) throws IOException {
      this.taskState = ReplicaMoverTaskState.RUNNING;
      if (!isSameDC(this.target, this.proxy) && crossDCThrottler != null) {
        // here means that MoverTask crosses dc.
        LOG.debug("[{}][Throttle] Acquire {}.", nnc.getNsId(), this.block.getNumBytes());
        crossDCThrottler.throttle(this.block.getNumBytes());
      }
      Socket sock = new Socket();
      DataOutputStream out = null;
      DataInputStream in = null;
      try {
        LOG.info("Start moving {}.", this);

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
            new StorageType[]{this.storageType}, new String[0]);
        IOStreamPair
            saslStreams = saslClient.socketSend(sock, unbufOut, unbufIn, km,
            accessToken, this.target);
        unbufOut = saslStreams.out;
        unbufIn = saslStreams.in;
        out = new DataOutputStream(new BufferedOutputStream(unbufOut, 4096));
        in = new DataInputStream(new BufferedInputStream(unbufIn, 4096));

        new Sender(out).replaceBlock(eb, this.storageType, accessToken,
            this.source.getDatanodeUuid(), this.proxy, null);
        receiveResponse(in);
        nnc.addBytesMoved(this.block.getNumBytes());
        LOG.info("Successfully moved {}", this);
        this.endTime = Time.monotonicNow();
        this.taskState = ReplicaMoverTaskState.SUCCESS;
      } catch (IOException e) {
        if (firstDispatch) {
          this.taskState = ReplicaMoverTaskState.PENDING;
          return true;
        } else {
          this.endTime = Time.monotonicNow();
          this.taskState = ReplicaMoverTaskState.FAILED;
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
      DataTransferProtos.BlockOpResponseProto
          response = DataTransferProtos.BlockOpResponseProto.parseFrom(vintPrefixed(in));
      while (response.getStatus() == DataTransferProtos.Status.IN_PROGRESS) {
        // read intermediate responses
        response = DataTransferProtos.BlockOpResponseProto.parseFrom(vintPrefixed(in));

        if (blockMoveTimeout > 0 && (Time.monotonicNow() - startTime > blockMoveTimeout)) {
          throw new IOException("Block " + this.block + " move timed out");
        }
      }
      String logInfo = "reportedBlock move is failed";
      DataTransferProtoUtil.checkBlockOpStatus(response, logInfo, true);
    }
  }

  /**
   * A class that receives ReplicaTasks and executes them across a number of mover threads.
   */
  private static class MoverManager {
    private final BlockingQueue<ReplicaMoveTask> taskBlockingQueue;
    private final Set<ReplicaMoveTask> runningTasks = ConcurrentHashMap.newKeySet();
    private final MoverThread[] movers;
    private final AtomicBoolean shouldRun = new AtomicBoolean(true);
    private final AtomicInteger referenceCount = new AtomicInteger(0);

    MoverManager(Configuration conf) {
      int queueSize = conf.getInt(DFSConfigKeys.DFS_DECOMMISSION_QUEUE_SIZE_KEY,
          DFSConfigKeys.DFS_DECOMMISSION_QUEUE_SIZE_DEFAULT);
      this.taskBlockingQueue = new LinkedBlockingQueue<>(queueSize);

      int handlerCount = conf.getInt(DFSConfigKeys.DFS_DECOMMISSION_HANDLER_COUNT_KEY,
          DFSConfigKeys.DFS_DECOMMISSION_HANDLER_COUNT_DEFAULT);
      this.movers = new MoverThread[handlerCount];
      for (int i = 0; i < handlerCount; i++) {
        movers[i] = new MoverThread(this.taskBlockingQueue, i, shouldRun, runningTasks);
        movers[i].start();
      }
    }

    public boolean noTasks() {
      return this.taskBlockingQueue.isEmpty() && this.runningTasks.isEmpty();
    }

    void incrReference() {
      this.referenceCount.incrementAndGet();
    }

    int descReference() {
      return this.referenceCount.decrementAndGet();
    }

    public void addTask(ReplicaMoveTask replicaMoveTask) throws IOException {
      try {
        this.taskBlockingQueue.put(replicaMoveTask);
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
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

  /**
   * A class that takes ReplicaTasks from queue and executes it.
   */
  private static class MoverThread extends Thread {
    private final BlockingQueue<ReplicaMoveTask> movingBlocks;
    private final AtomicBoolean shouldRun;
    private final Set<ReplicaMoveTask> runningTasks;

    MoverThread(BlockingQueue<ReplicaMoveTask> movingBlocks, int instanceNumber,
        AtomicBoolean shouldRun, Set<ReplicaMoveTask> runningTasks) {
      this.movingBlocks = movingBlocks;
      this.shouldRun = shouldRun;
      this.runningTasks = runningTasks;
      this.setName("Mover thread " + instanceNumber);
    }

    public void run() {
      while (shouldRun.get()) {
        ReplicaMoveTask replicaMoveTask = null;
        try {
          replicaMoveTask = this.movingBlocks.take();
          this.runningTasks.add(replicaMoveTask);
          boolean shouldRetry = replicaMoveTask.dispatch(true);
          if (shouldRetry) {
            LOG.info("Will retry move {}", replicaMoveTask);
            replicaMoveTask.chooseTarget();
            replicaMoveTask.chooseProxy();
            replicaMoveTask.dispatch(false);
          }
        } catch (InterruptedException e) {
          // ignore
        } catch (Exception e) {
          LOG.warn("Failed move {} with error message {}.", replicaMoveTask, e.getMessage());
          if (replicaMoveTask != null) {
            replicaMoveTask.callBack();
          }
        } finally {
          if (replicaMoveTask != null) {
            if (replicaMoveTask.getFullPath() != null) {
              ZoneProgressTracker.addByteCount(replicaMoveTask.getBlock().getNumBytes());
              ZoneProgressTracker.incrBlockCount();
              ZoneProgressTracker.dequeueFile(replicaMoveTask.getFullPath());
            }
          }
          this.runningTasks.remove(replicaMoveTask);
        }
      }
    }
  }

  private boolean isSameDC(DatanodeInfo dn1, DatanodeInfo dn2) {
    String dc1 = NetworkTopologyUtil.getDataCenter(dn1);
    String dc2 = NetworkTopologyUtil.getDataCenter(dn2);
    return dc1.equalsIgnoreCase(dc2);
  }

  private boolean isSameRack(DatanodeInfo dn1, DatanodeInfo dn2) {
    String rack1 = dn1.getNetworkLocation();
    String rack2 = dn2.getNetworkLocation();
    return rack1.equalsIgnoreCase(rack2);
  }
}
