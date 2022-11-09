package org.apache.hadoop.hdfs.server.zoneservice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.net.DFSNetworkTopologyWithDataCenter;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher;
import org.apache.hadoop.hdfs.server.balancer.KeyManager;
import org.apache.hadoop.hdfs.server.balancer.NameNodeConnector;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher.DDatanode.StorageGroup;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ConnectException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;

/**
 * Dispatching block replica moves between datacenters.
 */
public class ZoneDispatcher extends Dispatcher {

  private static final Logger LOG = LoggerFactory.getLogger(ZoneDispatcher.class);
  private final int blockDispatchAttempts;
  private final long blockDispatchRetryInterval;
  private final long dispatcherKeepAliveTime;
  private static final long DELAY_AFTER_DATANODE_ERRORS = 10 * 60 * 1000;
  protected final ExecutorService dispatchExecutor;

  /** Constructor called by ZoneMover. */
  public ZoneDispatcher(NameNodeConnector nnc, Set<String> includedNodes,
      Set<String> excludedNodes, long movedWinWidth,
      int dispatcherThreads, int maxConcurrentMovesPerNode,
      Configuration conf, int blockDispatchAttempts,
      long blockDispatchRetryInterval) {
    this(nnc, includedNodes, excludedNodes, movedWinWidth,
        0, dispatcherThreads, maxConcurrentMovesPerNode,
        0L, 0L, 0, 0, conf,
        blockDispatchAttempts, blockDispatchRetryInterval, 0);
  }

  public ZoneDispatcher(NameNodeConnector nnc, Set<String> includedNodes,
      Set<String> excludedNodes, long movedWinWidth,
      int dispatcherThreads, int maxConcurrentMovesPerNode,
      Configuration conf, int blockDispatchAttempts,
      long blockDispatchRetryInterval, long dispatcherKeepAliveTime) {
    this(nnc, includedNodes, excludedNodes, movedWinWidth,
        0, dispatcherThreads, maxConcurrentMovesPerNode,
        0L, 0L, 0, 0, conf,
        blockDispatchAttempts, blockDispatchRetryInterval, dispatcherKeepAliveTime);
  }

  ZoneDispatcher(NameNodeConnector nnc, Set<String> includedNodes,
      Set<String> excludedNodes, long movedWinWidth, int moverThreads,
      int dispatcherThreads, int maxConcurrentMovesPerNode,
      long getBlocksSize, long getBlocksMinBlockSize,
      int blockMoveTimeout, int maxNoMoveInterval,
      Configuration conf, int blockDispatchAttempts,
      long blockDispatchRetryInterval, long dispatcherKeepAliveTime) {
    super(nnc, includedNodes, excludedNodes,
        movedWinWidth, moverThreads, 0,
        maxConcurrentMovesPerNode, getBlocksSize, getBlocksMinBlockSize,
        blockMoveTimeout, maxNoMoveInterval, conf);
    this.blockDispatchAttempts = blockDispatchAttempts;
    this.blockDispatchRetryInterval = blockDispatchRetryInterval;
    this.dispatcherKeepAliveTime = dispatcherKeepAliveTime;
    this.dispatchExecutor = dispatcherThreads == 0? null
        : HadoopExecutors.newFixedThreadPool(dispatcherThreads, dispatcherKeepAliveTime);
  }

  /** This class keeps track of a scheduled block move */
  public class ZonePendingMove extends PendingMove {

    private ZonePendingMove(Source source, StorageGroup target) {
      super(source, target);
    }

    /**
     * Choose a proxy source.
     *
     * @return true if a proxy is found; otherwise false
     */
    @Override
    protected boolean chooseProxySource() {
      final DatanodeInfo targetDN = target.getDatanodeInfo();
      // if source and target are same nodes then no need of proxy
      if (source.getDatanodeInfo().equals(targetDN) && addTo(source)) {
        return true;
      }

      // if node group is supported, first try add nodes in the same node group
      if (cluster.isNodeGroupAware()) {
        for (StorageGroup loc : block.getLocations()) {
          if (cluster.isOnSameNodeGroup(loc.getDatanodeInfo(), targetDN)
              && addTo(loc)) {
            return true;
          }
        }
      }

      // check if there is replica which is on the same rack with the target
      for (StorageGroup loc : block.getLocations()) {
        if (cluster.isOnSameRack(loc.getDatanodeInfo(), targetDN) && addTo(loc)) {
          return true;
        }
      }

      // find a replica in the same datacenter
      String targetDC = DFSNetworkTopologyWithDataCenter.getDataCenter(
          target.getDatanodeInfo().getNetworkLocation());
      if (targetDC.equals(DFSNetworkTopologyWithDataCenter.getDataCenter(
          source.getDatanodeInfo().getNetworkLocation())) && addTo(source)) {
        return true;
      }
      List<StorageGroup> locations = block.getLocations();
      Collections.shuffle(locations);
      for (StorageGroup loc : locations) {
        if (targetDC.equals(DFSNetworkTopologyWithDataCenter.getDataCenter(
            loc.getDatanodeInfo().getNetworkLocation())) && addTo(loc)) {
          return true;
        }
      }

      // find out a non-busy replica
      for (StorageGroup loc : locations) {
        if (addTo(loc)) {
          return true;
        }
      }
      return false;
    }

    public StorageGroup getTarget() {
      return target;
    }

    /**
     * Dispatch and retry in block level
     */
    public void dispatchWithRetry() {
      try {
        for (int i=1; i<=blockDispatchAttempts; i++) {
          LOG.info("Try round " + i + " to move " + this);
          try {
            simpleDispatch();
            target.getDDatanode().setHasSuccess();
            nnc.addBytesMoved(block.getNumBytes());
            LOG.info("Successfully moved " + this + " at round " + i);
            return;
          } catch (SocketTimeoutException|ConnectException|InvalidBlockTokenException e) {
            // for InvalidBlockTokenException, please refer HDFS-13441
            LOG.warn("Failed to move " + this, e);
            LOG.warn("Found a problem datanode: " +
                target.getDDatanode().getDatanodeInfo());
            target.getDDatanode().setHasFailure();
            target.getDDatanode().activateDelay(DELAY_AFTER_DATANODE_ERRORS);
            return;
          } catch (IOException e) {
            // If the attempt encounters "IOException: Block move timed out",
            // it may encounter ReplicaAlreadyExistsException when retrying
            // if the first attempt actually succeed in the target datanode.
            if (e.getMessage().contains("ReplicaAlreadyExistsException")) {
              LOG.info("Ignore ReplicaAlreadyExistsException for " + this);
              return;
            }

            LOG.warn("Failed to move " + this, e);

            if (e.getMessage().contains("SocketTimeoutException") ||
                e.getMessage().contains("ConnectException") ||
                e.getMessage().contains("InvalidBlockTokenException")) {
              LOG.warn("Found a problem datanode: " + proxySource.getDatanodeInfo());
              target.getDDatanode().setHasFailure();
              proxySource.activateDelay(DELAY_AFTER_DATANODE_ERRORS);
              return;
            }

            // Proxy or target may have some issues, delay before using these nodes
            // further in order to avoid a potential storm of "threads quota
            // exceeded" warnings when the dispatcher gets out of sync with work
            // going on in datanodes.
            proxySource.activateDelay(delayAfterErrors);
            target.getDDatanode().activateDelay(delayAfterErrors);
            Thread.sleep(blockDispatchRetryInterval);
          }
        }
        LOG.warn("Failed to move " + this + " after " + blockDispatchAttempts + " rounds.");
        target.getDDatanode().setHasFailure();
      } catch (InterruptedException ie) {
        LOG.warn("Encountered InterruptedException, will skip " + this);
      } finally {
        proxySource.removePendingBlock(this);
        target.getDDatanode().removePendingBlock(this);

        synchronized (this) {
          reset();
        }
        synchronized (ZoneDispatcher.this) {
          ZoneDispatcher.this.notifyAll();
        }
      }
    }

    /**
     * Try dispatch without clean actions
     */
    private void simpleDispatch() throws IOException {
      Socket sock = new Socket();
      DataOutputStream out = null;
      DataInputStream in = null;
      try {
        sock.connect(
            NetUtils.createSocketAddr(target.getDatanodeInfo().
                getXferAddr(ZoneDispatcher.this.connectToDnViaHostname)),
            HdfsConstants.READ_TIMEOUT);

        // Set read timeout so that it doesn't hang forever against
        // unresponsive nodes. Datanode normally sends IN_PROGRESS response
        // twice within the client read timeout period (every 30 seconds by
        // default). Here, we make it give up after 5 minutes of no response.
        sock.setSoTimeout(HdfsConstants.READ_TIMEOUT * 5);
        sock.setKeepAlive(true);

        OutputStream unbufOut = sock.getOutputStream();
        InputStream unbufIn = sock.getInputStream();
        ExtendedBlock eb = new ExtendedBlock(nnc.getBlockpoolID(),
            block.getBlock());
        final KeyManager km = nnc.getKeyManager();
        Token<BlockTokenIdentifier> accessToken = km.getAccessToken(eb);
        IOStreamPair saslStreams = saslClient.socketSend(sock, unbufOut,
            unbufIn, km, accessToken, target.getDatanodeInfo());
        unbufOut = saslStreams.out;
        unbufIn = saslStreams.in;
        out = new DataOutputStream(new BufferedOutputStream(unbufOut,
            ioFileBufferSize));
        in = new DataInputStream(new BufferedInputStream(unbufIn,
            ioFileBufferSize));

        sendRequest(out, eb, accessToken);
        receiveResponse(in);
      } finally {
        IOUtils.closeStream(out);
        IOUtils.closeStream(in);
        IOUtils.closeSocket(sock);
      }
    }

    @Override
    protected boolean stopWaitingForResponse(long startTime) {
      return (blockMoveTimeout > 0 &&
          (Time.monotonicNow() - startTime > blockMoveTimeout));
    }
  }

  /** A node that can be the sources of a block move */
  public class ZoneSource extends Source {

    ZoneSource(StorageType storageType, long maxSize2Move, DDatanode dn) {
      super(storageType, maxSize2Move, dn);
    }

    @Override
    public PendingMove addPendingMove(DBlock block, StorageGroup target) {
      return target.addPendingMove(block, new ZonePendingMove(this, target));
    }
  }

  /** A class that keeps track of a datanode. */
  public static class ZoneDDatanode extends DDatanode {
    // limit concurrent moves on each target datanode
    private final Semaphore permits;
    private final int maxConcurrentMoves;

    private ZoneDDatanode(DatanodeInfo datanode, int maxConcurrentMoves) {
      super(datanode, maxConcurrentMoves);
      this.maxConcurrentMoves = maxConcurrentMoves;
      this.permits = new Semaphore(maxConcurrentMoves, true);
    }

    public ZoneSource addSource(StorageType storageType,
        long maxSize2Move, ZoneDispatcher d) {
      final ZoneSource zs = d.new ZoneSource(storageType, maxSize2Move, this);
      put(storageType, zs, sourceMap);
      return zs;
    }

    @Override
    public synchronized boolean addPendingBlock(PendingMove pendingBlock) {
      if (!isAlive) {
        return false;
      }

      int MAX_WAITING_MULTIPLE = 2;
      // avoid too many tasks waiting for the permit of this node
      if (getPendingSize() >= maxConcurrentMoves * MAX_WAITING_MULTIPLE) {
        return false;
      }
      return super.addPendingBlock(pendingBlock);
    }
  }

  /** Create a new ZoneDDatanode from DatanodeInfo */
  public ZoneDDatanode newZoneDDatanode(DatanodeInfo datanode) {
    return new ZoneDDatanode(datanode, maxConcurrentMovesPerNode);
  }

  /**
   * Decide if the block is a good candidate to be moved from source to target.
   * A block is a good candidate if
   * 1. the block does not have a replica on the target;
   * 2. doing the move does not reduce the number of racks that the block has
   */
  @Override
  protected boolean isGoodBlockCandidate(StorageGroup source, StorageGroup target,
      StorageType targetStorageType, DBlock block) {
    if (source.equals(target)) {
      return false;
    }

    if (target.getStorageType() != targetStorageType) {
      return false;
    }

    final DatanodeInfo targetDatanode = target.getDatanodeInfo();
    if (source.getDatanodeInfo().equals(targetDatanode)) {
      // the block is moved inside same DN
      return true;
    }

    // check if block has replica in target node
    for (StorageGroup blockLocation : block.getLocations()) {
      if (blockLocation.getDatanodeInfo().equals(targetDatanode)) {
        return false;
      }
    }

    return isGoodBlockCandidateForPlacementPolicy(source, target, block);
  }

  @Override
  public void executePendingMove(final PendingMove p) {
    if (!(p instanceof ZonePendingMove)) {
      throw new IllegalArgumentException("ZonePendingMove instance is needed!");
    }
    final ZonePendingMove zpv = (ZonePendingMove) p;
    dispatchExecutor.execute(new Runnable() {
      @Override
      public void run() {
        final ZoneDDatanode targetDn = (ZoneDDatanode) zpv.getTarget().getDDatanode();
        try {
          targetDn.permits.acquire();
          zpv.dispatchWithRetry();
          targetDn.permits.release();
        } catch (InterruptedException ie) {
          LOG.warn("Encountered InterruptedException, will skip " + zpv);
        }
      }
    });
  }

}
