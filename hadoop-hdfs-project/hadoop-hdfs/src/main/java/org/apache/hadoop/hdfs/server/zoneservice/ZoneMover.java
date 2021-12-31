package org.apache.hadoop.hdfs.server.zoneservice;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.net.DFSNetworkTopologyWithDataCenter;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher.*;
import org.apache.hadoop.hdfs.server.mover.Mover;
import org.apache.hadoop.hdfs.server.mover.Mover.MLocation;
import org.apache.hadoop.hdfs.server.mover.Mover.Result;
import org.apache.hadoop.hdfs.server.balancer.Dispatcher.DDatanode.StorageGroup;
import org.apache.hadoop.hdfs.server.balancer.ExitStatus;
import org.apache.hadoop.hdfs.server.balancer.NameNodeConnector;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyWithDataCenter;
import org.apache.hadoop.hdfs.server.namenode.UnsupportedActionException;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.zoneservice.ZoneDispatcher.ZoneSource;
import org.apache.hadoop.hdfs.server.zoneservice.ZoneDispatcher.ZoneDDatanode;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/** <p>The zonemover is a tool that supports migrating replicas of blocks
 * between datacenters.
 * </p>
 *
 * <pre>
 * To start:
 *      hdfs zonemover -namespace <namespace> -path <path> -rule <rule>
 */
public class ZoneMover {
  private static final Logger LOG = LoggerFactory.getLogger(ZoneMover.class);
  private static final String ROOT = "/";
  private static final Path ZONEMOVER_ID_PATH =
      new Path("/system/zonemover.id");
  private final ZoneDispatcher dispatcher;
  private final StorageMap storages;
  private final List<Path> targetPaths;
  private final int retryMaxAttempts;
  private final ReplicationRule rule;
  private final Map<String, Short> ruleMap;
  private final AtomicInteger retryCount;
  private final DFSClient dfs;
  private static final long DELAY_AFTER_CHOOSE_FAIL = 2 * 1000;
  private final Processor processor = new Processor();

  public ZoneMover(NameNodeConnector nnc, Configuration conf,
      ReplicationRule rule, AtomicInteger retryCount) {
    final long movedWinWidth = conf.getLong(
        DFSConfigKeys.DFS_ZONEMOVER_MOVEDWINWIDTH_KEY,
        DFSConfigKeys.DFS_ZONEMOVER_MOVEDWINWIDTH_DEFAULT);
    final int dispatcherThreads = conf.getInt(
        DFSConfigKeys.DFS_ZONEMOVER_DISPATCHERTHREADS_KEY,
        DFSConfigKeys.DFS_ZONEMOVER_DISPATCHERTHREADS_DEFAULT);
    final int maxConcurrentMovesPerNode = conf.getInt(
        DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY,
        DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_DEFAULT);
    this.retryMaxAttempts = conf.getInt(
        DFSConfigKeys.DFS_ZONEMOVER_RETRY_MAX_ATTEMPTS_KEY,
        DFSConfigKeys.DFS_ZONEMOVER_RETRY_MAX_ATTEMPTS_DEFAULT);
    this.rule = rule;
    this.ruleMap = rule.toMap();
    this.retryCount = retryCount;
    final int blockDispatchAttempts = conf.getInt(
        DFSConfigKeys.DFS_ZONEMOVER_BLOCK_DISPATCH_ATTEMPTS_KEY,
        DFSConfigKeys.DFS_ZONEMOVER_BLOCK_DISPATCH_ATTEMPTS_DEFAULT);
    final long blockDispatchRetryInterval = conf.getLong(
        DFSConfigKeys.DFS_ZONEMOVER_BLOCK_DISPATCH_RETRY_INTERVAL_KEY,
        DFSConfigKeys.DFS_ZONEMOVER_BLOCK_DISPATCH_RETRY_INTERVAL_DEFAULT);
    this.dispatcher = new ZoneDispatcher(nnc, Collections.<String> emptySet(),
        Collections.<String> emptySet(), movedWinWidth, dispatcherThreads,
        maxConcurrentMovesPerNode, conf,
        blockDispatchAttempts, blockDispatchRetryInterval);
    this.dfs = this.dispatcher.getDistributedFileSystem().getClient();
    this.storages = new StorageMap();
    this.targetPaths = nnc.getTargetPaths();
  }

  /**
   * Check if zonemover is compatible with the block placement policy
   * used by the NameNode.
   */
  private static void checkReplicationPolicyCompatibility(Configuration conf
  ) throws UnsupportedActionException {
    String clazz = conf.get(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY);
    if (clazz == null || !clazz.equals(
        BlockPlacementPolicyWithDataCenter.class.getName())) {
      throw new UnsupportedActionException(
          "ZoneMover must work with BlockPlacementPolicyWithDataCenter");
    }
  }

  DBlock newDBlock(Block block, List<MLocation> locations) {
    final DBlock db = new DBlock(block);
    for(MLocation ml : locations) {
      StorageGroup source = storages.getSource(ml);
      if (source != null) {
        db.addLocation(source);
      }
    }
    return db;
  }

  void init() throws IOException {
    LOG.info("Initializing ...");
    final List<DatanodeStorageReport> reports = dispatcher.init();
    LOG.info("Datanode reports size: " + reports.size());
    for (DatanodeStorageReport r: reports) {
      final ZoneDDatanode dn = dispatcher.newZoneDDatanode(r.getDatanodeInfo());
      for (StorageType t: StorageType.getMovableTypes()) {
        final ZoneSource source = dn.addSource(t, Long.MAX_VALUE, dispatcher);
        final long maxRemaining = Mover.getMaxRemaining(r, t);
        final StorageGroup target = maxRemaining > 0L ? dn.addTarget(t, maxRemaining) : null;
        storages.add(source, target);
      }
    }
  }

  /* run the process logic */
  ExitStatus run() {
    try {
      return new Processor().processPath().getExitStatus();
    } catch (IllegalArgumentException e) {
      System.out.println(e + ".  Exiting ...");
      return ExitStatus.ILLEGAL_ARGUMENTS;
    }
  }

  ExitStatus run(String path) {
    Result result = new Result();
    processor.processPath(path, result);
    return result.getExitStatus();
  }

  /* release resources */
  void shutdown() {
    dispatcher.shutdownNow();
  }

  /* reset success and failure states of targets */
  void resetTargetsStatus() {
    for (StorageGroup node: storages.targets.values()) {
      node.getDDatanode().resetStatus();
    }
  }

  /**
   * Run with prepared arguments.
   * @param conf configuration
   * @param namenode URI of the NameNode
   * @param paths paths to apply the rule
   * @param rule the rule to apply
   * @return a ExitStatus code
   */
  public static int run(Configuration conf, URI namenode, List<Path> paths, ReplicationRule rule)
      throws IOException, InterruptedException {
    LOG.info("Start to apply rule: " + rule + " to namenode:" + namenode + ", path: " + paths);

    NameNodeConnector nnc = null;
    ZoneMover zs = null;
    // retryCount starts from 0 and ends at retryMaxAttempts
    AtomicInteger retryCount = new AtomicInteger(0);
    final long sleepTime = calculateSleepTime(conf);
    final boolean exitEvenHasProgress = conf.getBoolean(
        DFSConfigKeys.DFS_ZONEMOVER_EXIT_EVEN_HAS_PROGRESS,
        DFSConfigKeys.DFS_ZONEMOVER_EXIT_EVEN_HAS_PROGRESS_DEFAULT);

    try {
      // Set maxNotChangedIterations to 1 as ZoneMover does not need to loop
      nnc = new NameNodeConnector(ZoneMover.class.getSimpleName(),
          namenode, ZONEMOVER_ID_PATH, paths, conf, 1);
      zs = new ZoneMover(nnc, conf, rule, retryCount);
      zs.init();
      int round = 0;

      while (true) {
        round += 1;
        LOG.info("Start round " + round + " ...");
        final ExitStatus r= zs.run();
        if (r == ExitStatus.SUCCESS) {
          break;
        } else if (r != ExitStatus.IN_PROGRESS) {
          if (r == ExitStatus.NO_MOVE_PROGRESS) {
            System.err.println("Failed to move some blocks after "
                + zs.retryMaxAttempts + " retries. Exiting...");
          } else if (r == ExitStatus.NO_MOVE_BLOCK) {
            System.err.println("Some blocks can't be moved. Exiting...");
          } else {
            System.err.println("ZoneMover failed. Exiting with status " + r + "... ");
          }
          // must be an error statue, return
          return r.getExitCode();
        } else if (exitEvenHasProgress) {
          // If we apply a replication rule to a path with a lot of data,
          // for example, more than 1 PB. Some replicas may encounter
          // moving failure but most succeed. At this case, the ExitStatus will
          // be IN_PROGRESS. It will cost a lot of time to go through all files
          // once more. We can exit here and rerun ZoneMover or not based on the
          // number of failing cases in the log.
          LOG.info("ZoneMover has progress in this round. Exiting ... ");
          return r.getExitCode();
        }
        zs.resetTargetsStatus();
        //noinspection BusyWait
        Thread.sleep(sleepTime);
      }
    } finally {
      if (nnc != null) {
        IOUtils.cleanupWithLogger(LOG, nnc);
      }
      if (zs != null) {
        zs.shutdown();
      }
    }

    return ExitStatus.SUCCESS.getExitCode();
  }

  /**
   * Run with prepared arguments for monitorByTrigger mode.
   * @param zoneMoverTrigger trigger for ZoneMover monitor
   * @param conf configuration
   * @param namenode URI of the NameNode
   * @param paths paths to apply the rule
   * @param rule the rule to apply
   * @return a ExitStatus code
   */
  public static int run(ZoneMoverTrigger zoneMoverTrigger, Configuration conf,
      URI namenode, List<Path> paths, ReplicationRule rule)
      throws IOException, InterruptedException{
    NameNodeConnector nnc = null;
    ZoneMover zs = null;
    try {
      nnc = new NameNodeConnector(ZoneMover.class.getSimpleName(),
              namenode, ZONEMOVER_ID_PATH, paths, conf, 1);
      zs = new ZoneMover(nnc, conf, rule, new AtomicInteger(0));
      zs.init();
      while (zoneMoverTrigger.hasNext()) {
        String curPath = zoneMoverTrigger.getNext();

        // process the path
        LOG.debug("Check path: " + curPath);
        ExitStatus exitStatus = zs.run(curPath);
        if (exitStatus != ExitStatus.SUCCESS) {
          LOG.warn("Monitor process file fail: " + curPath);
        }
      }
    } finally {
      if (nnc != null) {
        IOUtils.cleanupWithLogger(LOG, nnc);
      }
      if (zs != null) {
        zs.shutdown();
      }
    }
    return ExitStatus.SUCCESS.getExitCode();
  }

  private static long calculateSleepTime(final Configuration conf) {
    return conf.getLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
        DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT) * 1000 * 3 +
        conf.getLong(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY,
            DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_DEFAULT) * 1000 * 3;
  }

  /**
   * Get the datacenter distribution of the block.
   */
  static Map<String, Short> getBlockDistribution(LocatedBlock block) {
    Map<String, Short> distribution = new HashMap<>();
    // Count replica in each datacenter
    DatanodeInfo[] infos = block.getLocations();
    for (DatanodeInfo info: infos) {
      String dc = DFSNetworkTopologyWithDataCenter.getDataCenter(
          info.getNetworkLocation());
      if (distribution.containsKey(dc)) {
        distribution.put(dc, (short) (distribution.get(dc) + 1));
      } else {
        distribution.put(dc, (short) 1);
      }
    }
    return distribution;
  }

  /**
   * Check if a block already satisfies the rule.
   */
  boolean isBlockSatisfyRule(LocatedBlock block) {
    return rule.equals(
        ReplicationRule.parseFromMap(getBlockDistribution(block)));
  }

  /**
   * Check if all blocks in the list have the same datacenter distribution.
   */
  static boolean areBlocksDistributionConsistent(
      List<LocatedBlock> blocks) {
    boolean consistent = true;
    Map<String, Short> distribution = null;
    for (LocatedBlock block: blocks) {
      if (distribution == null) {
        distribution = getBlockDistribution(block);
      } else {
        if (!distribution.equals(getBlockDistribution(block))) {
          consistent = false;
          break;
        }
      }
    }
    return consistent;
  }

  /**
   * Get {@link ZoneMoveItem} list for the block.
   */
  List<ZoneMoveItem> getZoneMoveItems(final LocatedBlock block) {
    // calculate sources and targets
    Map<String, Short> distribution = getBlockDistribution(block);
    Map<String, Short> sources = new HashMap<>();
    Map<String, Short> targets = new HashMap<>();
    Set<String> dcs = new HashSet<>();
    dcs.addAll(distribution.keySet());
    dcs.addAll(ruleMap.keySet());
    for (String dc: dcs) {
      short m = 0;
      if (distribution.containsKey(dc)) {
        m = distribution.get(dc);
      }
      short n = 0;
      if (ruleMap.containsKey(dc)) {
        n = ruleMap.get(dc);
      }
      if (m > n) {
        sources.put(dc, (short) (m - n));
      } else if (m < n) {
        targets.put(dc, (short) (n - m));
      }
    }

    // generate ZoneMoveItem
    List<ZoneMoveItem> items = new ArrayList<>();
    for (Map.Entry<String, Short> sourceEntry: sources.entrySet()) {
      String sdc = sourceEntry.getKey();
      short x = sourceEntry.getValue();
      for (String tdc: new HashSet<>(targets.keySet())) {
        short y = targets.get(tdc);
        if (x == y) {
          items.add(new ZoneMoveItem(sdc, tdc, x));
          targets.remove(tdc);
          break;
        } else if (x > y) {
          items.add(new ZoneMoveItem(sdc, tdc, y));
          targets.remove(tdc);
          x = (short) (x - y);
        } else {
          items.add(new ZoneMoveItem(sdc, tdc, x));
          targets.put(tdc, (short) (y - x));
          break;
        }
      }
    }
    return items;
  }

  static class ZoneMoveItem {
    private final String sourceDataCenter;
    private final String targetDataCenter;
    private final short num;

    /**
     * @param sourceDataCenter source data center
     * @param targetDataCenter target data center
     * @param num replicas to move
     */
    ZoneMoveItem(@Nonnull final String sourceDataCenter,
        @Nonnull final String targetDataCenter, final short num) {
      this.sourceDataCenter = sourceDataCenter;
      this.targetDataCenter = targetDataCenter;
      this.num = num;
    }

    public String getSourceDataCenter() {
      return sourceDataCenter;
    }

    public String getTargetDataCenter() {
      return targetDataCenter;
    }

    public short getNum() {
      return num;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ZoneMoveItem that = (ZoneMoveItem) o;
      return num == that.num && sourceDataCenter.equals(
          that.sourceDataCenter) && targetDataCenter.equals(that.targetDataCenter);
    }

    @Override
    public int hashCode() {
      return Objects.hash(sourceDataCenter, targetDataCenter, num);
    }

    @Override
    public String toString() {
      return "ZoneMoveItem{" +
          "source='" + sourceDataCenter + '\'' +
          ", target='" + targetDataCenter + '\'' +
          ", num=" + num +
          '}';
    }
  }

  /* Keep StorageGroupMap for sources and targets */
  private static class StorageMap {
    private final StorageGroupMap<ZoneSource> sources
        = new StorageGroupMap<>();
    private final StorageGroupMap<StorageGroup> targets
        = new StorageGroupMap<>();
    private final HashMap<String, EnumMap<StorageType, List<StorageGroup>>>
        dcTargetStorageTypeMap = new HashMap<>();

    private StorageMap() {}

    private void add(ZoneSource source, StorageGroup target) {
      sources.put(source);
      if (target != null) {
        targets.put(target);
        String dc = DFSNetworkTopologyWithDataCenter.getDataCenter(
            source.getDatanodeInfo().getNetworkLocation());
        getTargetStorages(target.getStorageType(), dc).add(target);
      }
    }

    private ZoneSource getSource(MLocation ml) {
      return get(sources, ml);
    }

    private StorageGroup getTarget(MLocation ml) {
      return get(targets, ml);
    }

    private static <G extends StorageGroup> G get(StorageGroupMap<G> map, MLocation ml) {
      return map.get(ml.getDatanode().getDatanodeUuid(), ml.getStorageType());
    }

    private List<StorageGroup> getTargetStorages(StorageType st, String datacenter) {
      if (!dcTargetStorageTypeMap.containsKey(datacenter)) {
        EnumMap<StorageType, List<StorageGroup>> em =
            new EnumMap<>(StorageType.class);
        for(StorageType t : StorageType.getMovableTypes()) {
          em.put(t, new LinkedList<StorageGroup>());
        }
        dcTargetStorageTypeMap.put(datacenter, em);
      }
      return dcTargetStorageTypeMap.get(datacenter).get(st);
    }
  }

  class Processor {

    private Result processPath() {
      Result result = new Result();
      for (Path target: targetPaths) {
        processPath(target.toUri().getPath(), result);
      }
      // wait for pending move to finish and retry the failed migration
      boolean hasFailed = Dispatcher.waitForMoveCompletion(storages.targets.values());
      boolean hasSuccess = Dispatcher.checkForSuccess(storages.targets.values());

      // check and update retryCount
      if (hasFailed && !hasSuccess) {
        if (retryCount.get() == retryMaxAttempts) {
          result.setRetryFailed();
          LOG.error("Failed to move some block's after "
              + retryMaxAttempts + " retries.");
          return result;
        } else {
          retryCount.incrementAndGet();
        }
      } else {
        // Reset retry count if no failure or have success.
        retryCount.set(0);
      }

      if (hasFailed) {
        result.updateHasRemaining(true);
      }

      return result;
    }

    private void processPath(String fullPath, Result result) {
      LOG.info("Processing path: " + fullPath + " ...");
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
        for (HdfsFileStatus child : children.getPartialListing()) {
          processRecursively(fullPath, child, result);
        }
        if (children.hasMore()) {
          lastReturnedName = children.getLastName();
        } else {
          return;
        }
      }
    }

    private void processRecursively(
        String parent, HdfsFileStatus status, Result result) {
      String fullPath = status.getFullName(parent);
      if (status.isDir()) {
        if (!fullPath.endsWith(Path.SEPARATOR)) {
          fullPath = fullPath + Path.SEPARATOR;
        }
        processPath(fullPath, result);
      } else if (!status.isSymlink()) { // file
        processFile(fullPath, (HdfsLocatedFileStatus) status, result);
      }
    }

    private void processFile(String fullPath,
        HdfsLocatedFileStatus status, Result result) {
      LOG.info("Processing file: " + fullPath + " ....");

      if (status.getReplication() != rule.getReplica()) {
        LOG.warn("Skip replication inconsistent file: " + fullPath);
        return;
      }

      final LocatedBlocks locatedBlocks = status.getBlockLocations();
      int n = locatedBlocks.locatedBlockCount();
      if (n == 0) {
        LOG.info("Skip empty file: " + fullPath);
        return;
      }

      if (!locatedBlocks.isLastBlockComplete()) {
        LOG.info("Skip uncompleted file: " + fullPath);
        return;
      }

      // get the first block
      LocatedBlock firstBlock = locatedBlocks.get(0);
      // Cannot just check the first and last block, as the dispatching action
      // is parallel and asynchronous. The movement of any blocks of a file
      // may fail but other blocks succeed.
      if (areBlocksDistributionConsistent(locatedBlocks.getLocatedBlocks())) {
        if (isBlockSatisfyRule(firstBlock)) {
          LOG.info("Skip the file as it already satisfies the rule: " + fullPath);
          return;
        }
        processConsistentBlocks(locatedBlocks, result);
      } else {
        processInconsistentBlocks(locatedBlocks, result);
      }
    }

    /**
     * Process blocks have the same datacenter distribution.
     */
    private void processConsistentBlocks(
        LocatedBlocks locatedBlocks, Result result) {
      LocatedBlock firstBlock = locatedBlocks.get(0);
      if (isBlockSatisfyRule(firstBlock)) {
        return;
      }
      List<ZoneMoveItem> moveItems = getZoneMoveItems(firstBlock);
      int n = locatedBlocks.locatedBlockCount();
      for (int i=0; i<n; i++) {
        LocatedBlock block = locatedBlocks.get(i);
        if (scheduleMoves4Block(block, moveItems)) {
          result.setNoBlockMoved(false);
        } else {
          result.updateHasRemaining(true);
        }
      }
    }

    /**
     * Process blocks have different datacenter distributions.
     */
    private void processInconsistentBlocks(
        LocatedBlocks locatedBlocks, Result result) {
      int n = locatedBlocks.locatedBlockCount();
      for (int i=0; i<n; i++) {
        LocatedBlock block = locatedBlocks.get(i);
        if (isBlockSatisfyRule(block)) {
          continue;
        }
        if (scheduleMoves4Block(block, getZoneMoveItems(block))) {
          result.setNoBlockMoved(false);
        } else {
          result.updateHasRemaining(true);
        }
      }
    }

    boolean scheduleMoves4Block(LocatedBlock lb, List<ZoneMoveItem> moveItems) {
      final List<MLocation> locations = MLocation.toLocations(lb);
      // put locations to a map with datacenter as the key
      final Map<String, List<MLocation>> locationMap = new HashMap<>();
      for (MLocation ml: locations) {
        String dc = DFSNetworkTopologyWithDataCenter.getDataCenter(
            ml.getDatanode().getNetworkLocation());
        if (locationMap.containsKey(dc)) {
          locationMap.get(dc).add(ml);
        } else {
          locationMap.put(dc, new ArrayList<>(Collections.singletonList(ml)));
        }
      }

      final DBlock db = newDBlock(lb.getBlock().getLocalBlock(), locations);
      Set<StorageType> targetTypes = new HashSet<>(Arrays.asList(lb.getStorageTypes()));
      Set<StorageGroup> excluded = getExcluded(locations, moveItems);
      // get MLocation according to datacenter and select source
      for (ZoneMoveItem moveItem: moveItems) {
        for (short i=0; i<moveItem.getNum(); i++) {
          List<MLocation> sourceLocations = locationMap.get(moveItem.getSourceDataCenter());
          MLocation location = sourceLocations.get(0);
          sourceLocations.remove(0);
          ZoneSource source = storages.getSource(location);
          if (source != null) {
            if (!scheduleMoveReplica(db, source,
                moveItem.getTargetDataCenter(), targetTypes, excluded)) {
              return false;
            }
          } else {
            LOG.warn("Failed to get a source for : " + location
                + ", will skip this replica");
          }
        }
      }

      return true;
    }

    private Set<StorageGroup> getExcluded(
        List<MLocation> locations, List<ZoneMoveItem> moveItems) {
      Set<String> targetDataCenters = new HashSet<>();
      for (ZoneMoveItem item: moveItems) {
        targetDataCenters.add(item.targetDataCenter);
      }

      Set<StorageGroup> excluded = new HashSet<>();
      for (MLocation ml: locations) {
        String dc = DFSNetworkTopologyWithDataCenter.getDataCenter(
            ml.getDatanode().getNetworkLocation());
        if (targetDataCenters.contains(dc)) {
          excluded.add(storages.getTarget(ml));
        }
      }
      return excluded;
    }

    boolean scheduleMoveReplica(DBlock db, ZoneSource source, String targetDataCenter,
        Set<StorageType> targetTypes, Set<StorageGroup> excluded) {
      return chooseTargetInDataCenter(
          db, source, targetDataCenter, targetTypes, excluded);
    }

    /**
     * Choose a storage in the datacenter.
     */
    boolean chooseTargetInDataCenter(
        DBlock db, ZoneSource source, String targetDataCenter,
        Set<StorageType> targetTypes, Set<StorageGroup> excluded) {
      for (StorageType t: targetTypes) {
        final List<StorageGroup> targets = storages.getTargetStorages(t, targetDataCenter);
        Collections.shuffle(targets);
        for (StorageGroup target: targets) {
          if (excluded.contains(target)) {
            continue;
          }
          final PendingMove pm = source.addPendingMove(db, target);
          if (pm != null) {
            dispatcher.executePendingMove(pm);
            excluded.add(target);
            return true;
          }
        }
      }
      LOG.warn("chooseTargetInDataCenter failed with block: " +
          db + ", source: " + source + ", targetDataCenter: " + targetDataCenter +
          ",targetTypes: " + targetTypes + ", excluded: " + excluded);
      handleChooseFail(targetDataCenter, targetTypes);
      return false;
    }

    void handleChooseFail(String targetDataCenter, Set<StorageType> targetTypes) {
      for (StorageType t: targetTypes) {
        final List<StorageGroup> targets = storages.getTargetStorages(t, targetDataCenter);
        int total = 0;
        for (StorageGroup target: targets) {
          total += target.getDDatanode().getPendingSize();
        }
        LOG.info("Average pending size for targetDataCenter: " + targetDataCenter +
            ", storageType: " + t + ", datanodes.num: "+ targets.size() +
            " is " + total / targets.size());
      }
      try {
        Thread.sleep(DELAY_AFTER_CHOOSE_FAIL);
      } catch (InterruptedException e) {
        // ignore
      }
    }
  }

  static class Cli extends Configured implements Tool {
    private static final String USAGE = "Usage: hdfs zonemover"
        + "\n\t[-namespace <namespace>]\tthe namespace to apply the rule"
        + "\n\t-path <path>\tthe path to apply the rule"
        + "\n\t-pathFile <pathFile>\t the file contains paths to apply the rule"
        + "\n\t-rule <rule>\tthe replication rule"
        + "\n\t-monitor\tenable monitor mode"
        + "\n\t-monitorByTrigger\tenable monitor mode with trigger";

    private static Options buildCliOptions() {
      Options options = new Options();
      Option option = new Option(
          null, "namespace", true, "the namespace to apply the rule");
      options.addOption(option);

      OptionGroup pathGroup = new OptionGroup();
      pathGroup.setRequired(true);
      option = new Option(null, "path", true, "the path to apply the rule");
      pathGroup.addOption(option);
      option = new Option(null, "pathFile", true,
          "the file contains paths to apply the rule");
      pathGroup.addOption(option);
      options.addOptionGroup(pathGroup);

      option = new Option(null, "rule", true, "the replication rule");
      option.setRequired(true);
      options.addOption(option);

      option = new Option(null, "monitor", false, "enable monitor mode");
      options.addOption(option);

      option = new Option(null, "monitorByTrigger", false, "enable monitor by trigger");
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
     * Get {@link ReplicationRule} from command line
     */
    private static ReplicationRule getRule(CommandLine line)
        throws IllegalArgumentException {
      return ReplicationRule.parseFromString(line.getOptionValue("rule"));
    }

    private static List<Path> getPaths(CommandLine line)
        throws IllegalArgumentException, IOException {
      List<String> rawPaths;
      if (line.hasOption("path")) {
        rawPaths = new ArrayList<>(Collections.singletonList(line.getOptionValue("path")));
      } else {
        rawPaths = readPathFile(line.getOptionValue("pathFile"));
      }
      List<Path> paths = new ArrayList<>();
      for (String path: rawPaths) {
        if (!path.startsWith(ROOT)) {
          throw new IllegalArgumentException("Invalid path: " + path);
        }
        paths.add(new Path(path));
      }
      return paths;
    }

    private static List<String> readPathFile(String file) throws IOException {
      List<String> list = new ArrayList<>();
      BufferedReader reader = new BufferedReader(
          new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8));
      try {
        String line;
        while ((line = reader.readLine()) != null) {
          if (!line.trim().isEmpty()) {
            list.add(line);
          }
        }
      } finally {
        IOUtils.cleanupWithLogger(LOG, reader);
      }
      return list;
    }

    @Override
    public int run(String[] args) {
      long startTime = Time.monotonicNow();
      final Configuration conf = getConf();
      final Options options = buildCliOptions();
      CommandLineParser parser = new GnuParser();
      final long monitorCheckInterval = conf.getLong(
          DFSConfigKeys.DFS_ZONEMOVER_MONITOR_CHECK_INTERVAL_KEY,
          DFSConfigKeys.DFS_ZONEMOVER_MONITOR_CHECK_INTERVAL_DEFAULT);

      try {
        checkReplicationPolicyCompatibility(conf);
        CommandLine commandLine = parser.parse(options, args, true);
        URI namenode = getNamespaceUri(commandLine, conf);
        List<Path> paths = getPaths(commandLine);
        ReplicationRule rule = getRule(commandLine);
        if (commandLine.hasOption("monitorByTrigger")) {
          ZoneMoverTrigger zoneMoverTrigger =
              new ZoneMoverKafkaTrigger(conf, paths);
          return run(zoneMoverTrigger, conf, namenode, paths, rule);
        } else if (commandLine.hasOption("monitor")) {
          //noinspection InfiniteLoopStatement
          while (true) {
            startTime = Time.monotonicNow();
            run(conf, namenode, paths, rule);
            LOG.info("ZoneMover took "
                + StringUtils.formatTime(Time.monotonicNow() - startTime));
            //noinspection BusyWait
            Thread.sleep(monitorCheckInterval);
          }
        } else {
          return run(conf, namenode, paths, rule);
        }
      } catch (IOException e) {
        System.out.println(e + ".  Exiting ...");
        return ExitStatus.IO_EXCEPTION.getExitCode();
      } catch (ParseException | IllegalArgumentException e) {
        System.out.println(e + ".  Exiting ...");
        return ExitStatus.ILLEGAL_ARGUMENTS.getExitCode();
      } catch (InterruptedException e) {
        System.out.println(e + ".  Exiting ...");
        return ExitStatus.INTERRUPTED.getExitCode();
      } finally {
        LOG.info("ZoneMover took "
            + StringUtils.formatTime(Time.monotonicNow() - startTime));
      }
    }

    /**
     * Run with prepared arguments.
     */
    int run(Configuration conf, URI namenode, List<Path> paths, ReplicationRule rule)
        throws IOException, InterruptedException {
      return ZoneMover.run(conf, namenode, paths, rule);
    }

    /**
     * Run with ZoneMoverTrigger for monitorByTrigger mode
     */
    int run(ZoneMoverTrigger zoneMoverTrigger, Configuration conf,
        URI namenode, List<Path> paths, ReplicationRule rule)
        throws IOException, InterruptedException {
      return ZoneMover.run(zoneMoverTrigger, conf, namenode, paths, rule);
    }
  }

  public static void main(String[] args) {
    if (DFSUtil.parseHelpArgument(args, Cli.USAGE, System.out, true)) {
      System.exit(0);
    }

    try {
      System.exit(ToolRunner.run(new HdfsConfiguration(), new Cli(), args));
    } catch (Throwable e) {
      LOG.error("Exiting ZoneMover due to an exception", e);
      System.exit(-1);
    }
  }
}
