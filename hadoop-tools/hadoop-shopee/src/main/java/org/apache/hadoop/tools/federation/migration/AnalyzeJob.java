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
package org.apache.hadoop.tools.federation.migration;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.OpenFileEntry;
import org.apache.hadoop.hdfs.protocol.OpenFilesIterator;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One of the supported commands for {@link NSMigrationTool}.
 * Traverses a DFS subtree, finds bottom level directories (dirs that contain only files or empty)
 * that haven't been modified in a period of time. Then merges back up as far as possible to create
 * a minimal set of cold directories.
 */
public class AnalyzeJob {
  private static final Logger LOG = LoggerFactory.getLogger(AnalyzeJob.class);

  // HDFS stuff
  private final DistributedFileSystem router;
  private final DistributedFileSystem srcFs;
  private final DistributedFileSystem dstFs;
  private final Path input;
  private final Path output;
  private final long ms;

  // Threading stuff
  private final ExecutorService threadPool;
  private final AtomicInteger pathsOngoing;

  /**
   * {@link ConcurrentLinkedQueue} of {@link RawNodeData} representing cold directories,
   * to be populated during recursive traversal, and to be drained by {@link TreeProcessor}.
   */
  private final ConcurrentLinkedQueue<RawNodeData> results;
  /** Latch to prevent {@link TreeProcessor} from prematurely quitting. */
  private final CountDownLatch latch = new CountDownLatch(1);

  // Logging stuff
  private final AtomicInteger pathsChecked;
  private volatile long lastProgressLog;

  public AnalyzeJob(String path, String srcNs, String dstNs, String fedNs, String threshold,
      Path output, int concurrency, Configuration conf) throws IOException {
    this.router = (DistributedFileSystem) FileSystem.get(URI.create("hdfs://" + fedNs), conf);
    this.srcFs = (DistributedFileSystem) FileSystem.get(URI.create("hdfs://" + srcNs), conf);
    if (!dstNs.equals(srcNs)) {
      this.dstFs = (DistributedFileSystem) FileSystem.get(URI.create("hdfs://" + dstNs), conf);
    } else {
      this.dstFs = null;
    }
    this.input = new Path(path);
    this.output = output;
    this.results = new ConcurrentLinkedQueue<>();
    this.ms = Long.parseLong(threshold) * 86400 * 1000;
    this.pathsChecked = new AtomicInteger();
    this.lastProgressLog = Time.monotonicNow();
    this.threadPool = new ForkJoinPool(concurrency);
    // One for the root path, one as a flag
    this.pathsOngoing = new AtomicInteger(2);
    LOG.info("Analyzing path hdfs://{}{} with threshold {}, output {}", fedNs, path, threshold,
        output);
  }

  public static int handleArgs(List<String> argsList, Configuration conf)
      throws IOException, InterruptedException {
    String path = StringUtils.popOptionWithArgument("-path", argsList);
    if (path == null) {
      System.err.println("-path option is required.");
      return -1;
    }
    String src = StringUtils.popOptionWithArgument("-src", argsList);
    if (src == null) {
      System.err.println("-src option is required.");
      return -1;
    }
    String dst = StringUtils.popOptionWithArgument("-dst", argsList);
    if (dst == null) {
      dst = src;
    }
    String fed = StringUtils.popOptionWithArgument("-fed", argsList);
    if (fed == null) {
      fed = src;
    }
    String threshold = StringUtils.popOptionWithArgument("-days", argsList);
    if (threshold == null) {
      System.err.println("Cold data threshold is required with -days option.");
      return -1;
    }
    String outputFile = StringUtils.popOptionWithArgument("-output", argsList);
    if (outputFile == null) {
      System.err.println("An output file must be defined with -output.");
      return -1;
    }
    int concurrency = 64;
    String concurrencyStr = StringUtils.popOptionWithArgument("-concurrency", argsList);
    if (concurrencyStr != null) {
      concurrency = Integer.parseInt(concurrencyStr);
    }
    AnalyzeJob job =
        new AnalyzeJob(path, src, dst, fed, threshold, new Path(outputFile), concurrency, conf);
    job.execute();
    return 0;
  }

  /**
   * Short-lived container class that contains the path, to be consumed by {@link TreeProcessor}.
   * See {@link AnalyzeSubroutine} to see what data is stored by the arrays.
   */
  static class RawNodeData {
    private final String path;
    private final int[] requirements;
    // Note: the next 2 arrays have 1 more element than int[] requirements
    private final int[] fileCounts;
    private final long[] fileSizes;
    private final boolean isEmpty;

    RawNodeData(String path, int[] requirements, int[] fileCounts, long[] fileSizes,
        boolean isEmpty) {
      this.path = path;
      this.requirements = requirements;
      this.fileCounts = fileCounts;
      this.fileSizes = fileSizes;
      this.isEmpty = isEmpty;
    }

    private void printDebug() {
      LOG.debug("path={},reqs={},counts={},sizes={},empty={}", path, requirements, fileCounts,
          fileSizes, isEmpty);
    }
  }

  /**
   * Recursively traverses through a subtree starting at {@link AnalyzeJob#input},
   * populates {@link AnalyzeJob#results}.
   * <br>
   * int[] requirements is the number of cold dirs
   * of the same level to consider parent also a cold dir.
   * <pre>
   *   E.g. a path /projects/proj/a/b/c/d with requirements [3,7,2] means
   *               /projects/proj/a/b/c needs 2 children (/projects/proj/a/b/c/d being one of them)
   *                                    to be cold for the path itself to be cold
   *               /projects/proj/a/b   needs 7 children to be cold for the path to be cold
   *               /projects/proj/a     needs 3 children to be cold
   *               /projects/proj       is the input and will not be considered cold
   * </pre>
   * <br>
   * int[] fileCounts and long[] fileSizes are similar to int[] requirements,
   * the difference being these arrays contain the number/total size of files at each directory,
   * not counting subdirs.
   * <pre>
   *   E.g. a path /projects/proj/a/b/c/d with fileCounts [5,0,100,200]
   *                                       and fileSizes [1000,0,2000,5000] means
   *               /projects/proj/a/b/c/d has 200 files and they add up to 5000 bytes
   *               /projects/proj/a/b/c   has 100 files and they add up to 2000 bytes
   *               /projects/proj/a/b     has no files
   *               /projects/proj/a       has 5 files and they add up to 1000 bytes
   *               /projects/proj         is excluded from this calculation
   * </pre>
   */
  class AnalyzeSubroutine implements Runnable {

    String path;
    long mTime;
    int[] requirements;
    int[] fileCounts;
    long[] fileSizes;

    AnalyzeSubroutine(String path, long mTime, int[] requirements, int[] fileCounts,
        long[] fileSizes) {
      this.path = path;
      this.mTime = mTime;
      this.requirements = requirements;
      this.fileCounts = fileCounts;
      this.fileSizes = fileSizes;
    }

    @Override
    public void run() {
      FileStatus[] statuses;
      try {
        statuses = router.listStatus(new Path(path));
      } catch (FileNotFoundException fnfe) {
        // Path disappeared during analysis job. Likely hot dir in this case.
        LOG.info("Path {} not found", path);
        countAndPrintProgress();
        pathsOngoing.decrementAndGet();
        return;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      // Count all the cold files first
      int coldDirsNeeded = statuses.length;
      int fileCount = 0;
      long fileSize = 0;
      for (FileStatus status : statuses) {
        if (status.isFile() && Time.now() - mTime > ms
            && Time.now() - status.getModificationTime() > ms) {
          coldDirsNeeded--;
          fileCount++;
          fileSize += status.getLen();
        }
      }

      // Count progress here after checking the dir listing
      countAndPrintProgress();

      // If everything is cold files, the dir is cold dir
      if (coldDirsNeeded == 0) {
        int[] currentFileCounts = new int[fileCounts.length + 1];
        long[] currentFileSizes = new long[fileSizes.length + 1];
        System.arraycopy(fileCounts, 0, currentFileCounts, 0, fileCounts.length);
        System.arraycopy(fileSizes, 0, currentFileSizes, 0, fileSizes.length);
        currentFileCounts[fileCounts.length] = fileCount;
        currentFileSizes[fileSizes.length] = fileSize;
        results.add(new RawNodeData(path, requirements, currentFileCounts, currentFileSizes,
            statuses.length == 0));
        pathsOngoing.decrementAndGet();
        return;
      }

      for (FileStatus status : statuses) {
        if (!status.isFile()) {
          int[] currentRequirements = new int[requirements.length + 1];
          int[] currentFileCounts = new int[fileCounts.length + 1];
          long[] currentFileSizes = new long[fileSizes.length + 1];
          System.arraycopy(requirements, 0, currentRequirements, 0, requirements.length);
          System.arraycopy(fileCounts, 0, currentFileCounts, 0, fileCounts.length);
          System.arraycopy(fileSizes, 0, currentFileSizes, 0, fileSizes.length);
          currentRequirements[requirements.length] = coldDirsNeeded;
          currentFileCounts[fileCounts.length] = fileCount;
          currentFileSizes[fileSizes.length] = fileSize;
          pathsOngoing.incrementAndGet();
          threadPool.submit(new AnalyzeSubroutine(status.getPath().toUri().getPath(),
              status.getModificationTime(), currentRequirements, currentFileCounts,
              currentFileSizes));
        }
      }
      pathsOngoing.decrementAndGet();
    }

    private void countAndPrintProgress() {
      int count = pathsChecked.incrementAndGet();
      if (count % 10000 == 0) {
        long now = Time.monotonicNow();
        LOG.info("{} paths checked, {}ms since last time.", count, now - lastProgressLog);
        lastProgressLog = now;
      }
    }
  }

  public void execute() throws IOException, InterruptedException {
    FileStatus baseStatus = router.getFileStatus(input);
    Runnable task = new AnalyzeSubroutine(baseStatus.getPath().toUri().getPath(),
        baseStatus.getModificationTime(), new int[0], new int[0], new long[0]);
    threadPool.execute(task);
    TreeProcessor processor = new TreeProcessor();
    processor.start();
    pathsOngoing.decrementAndGet();
    while (pathsOngoing.get() > 0) {
      Thread.sleep(100);
    }
    latch.countDown();
    processor.join();
    threadPool.shutdown();
  }

  /**
   * Augmented node with an integer value indicating how many of its children nodes need to be
   * cold for the node itself to be cold. Do note that this value is different from
   * {@link RawNodeData} where the parent's requirement is stored
   * in the child path instead for ease of value propagation during traversal, whereas this class
   * stores the requirement of a path directly in the node it represents for better code
   * readability.
   * <pre>
   *   i.e. /projects/proj/a/b/c/d requirements [3,3,2]
   *          is expanded into
   *        root        count = -1
   *        "projects"  count = -1
   *        "proj"      count = -1
   *        "a"         count = 3
   *        "b"         count = 3
   *        "c"         count = 2
   *        "d"         count = 0
   * </pre>
   */
  static class NodeWithCount {
    String name;
    int count;
    int files;
    long size;
    NodeWithCount parent;
    Map<String, NodeWithCount> children = null;
    boolean empty;

    NodeWithCount(NodeWithCount parent, String name, int count, int files, long size,
        boolean empty) {
      this.parent = parent;
      this.name = name;
      this.count = count;
      this.files = files;
      this.size = size;
      this.empty = empty;
    }

    /**
     * Squashes a subtree starting from this node to the minimum tree.
     */
    void minimizeNode() {
      // Leaf or root node
      if (children == null) {
        assert count <= 0;
        return;
      }
      // Recursively trim
      for (NodeWithCount child : children.values()) {
        child.minimizeNode();
      }
      for (NodeWithCount child : children.values()) {
        // Cannot minimize anymore if child node is not cold
        if (child.count > 0) {
          return;
        }
      }
      // All children are now cold, if children count satisfies requirement, self is also cold
      if (children.size() == count) {
        if (children.values().stream().allMatch(x -> x.empty)) {
          this.empty = true;
        }
        // Special case, /projects/project_name or /user/username node, do not squash
        if ((parent.name.equals("projects") || parent.name.equals("user"))
            && parent.parent.name.isEmpty()) {
          return;
        }
        for (NodeWithCount child : children.values()) {
          files += child.files;
          size += child.size;
        }
        children.clear();
        children = null;
        count = 0;
      }
    }

    /**
     * Adds a path to this node. Should only be called from the root "/" node.
     */
    void addPath(RawNodeData raw) {
      String[] components = raw.path.split("/");

      NodeWithCount cur = this;
      for (int i = 1; i < components.length; i++) {
        final String component = components[i];
        NodeWithCount finalCur = cur;
        if (cur.children == null) {
          cur.children = new HashMap<>();
        }
        cur = finalCur.children.compute(component,
            (k, v) -> v == null ? new NodeWithCount(finalCur, component, -1, 0, 0, false) : v);
      }
      // Update leaf node with some values
      cur.empty = raw.isEmpty;
      cur.count = 0;
      cur.files = raw.fileCounts[raw.fileCounts.length - 1];
      cur.size = raw.fileSizes[raw.fileSizes.length - 1];

      // Backtrack to fill the counts
      cur = cur.parent;
      for (int i = raw.requirements.length - 1; i >= 0; i--) {
        cur.count = raw.requirements[i];
        // Note: fileCounts and fileSizes have 1 more element than requirements
        cur.files = raw.fileCounts[i];
        cur.size = raw.fileSizes[i];
        cur = cur.parent;
      }
    }

    /**
     * Increments the requirement for a node, or drops the node if it's a leaf node.
     * Should only be called from the root "/" node.
     */
    void incrRequirement(OpenFileEntry openFile) {
      String[] components = openFile.getFilePath().split("/");
      NodeWithCount cur = this;
      // /projects/proj/a/b/c/d/file -> [projects, proj, a, b, c, d]
      for (int i = 1; i < components.length - 1; i++) {
        final String component = components[i];
        // If node doesn't exist in cold tree, it has to be an already hot node, can ignore.
        if (cur.children == null) {
          cur.parent.children.remove(cur.name);
          cur.parent = null;
          return;
        }
        if (!cur.children.containsKey(component)) {
          return;
        }
        cur = cur.children.get(component);
      }
      // If can fully traverse the tree, increment requirement if not leaf node, else drop the node
      if (cur.children == null && cur.count == 0) {
        cur.parent.children.remove(cur.name);
        cur.parent = null;
      } else {
        cur.count++;
      }
    }

    Map<String, Pair<Integer, Long>> getAllLeafNodes(Map<String, Pair<Integer, Long>> result,
        List<String> components) {
      // Do not include empty dirs
      if (empty) {
        return result;
      }
      if (count == 0) {
        result.put("/" + String.join("/", components), Pair.of(files, size));
      } else {
        for (NodeWithCount node : children.values()) {
          List<String> subdirComponents = new ArrayList<>(components);
          subdirComponents.add(node.name);
          node.getAllLeafNodes(result, subdirComponents);
        }
      }
      return result;
    }
  }

  /**
   * Consumer class that keeps polling from {@link AnalyzeJob#results}, reconstructs a tree
   * of {@link NodeWithCount} to create the minimal set of cold directory. First, reconstructs
   * a full tree then collapses as many {@link NodeWithCount} as possible into parent nodes.
   * <pre>
   *   E.g. /projects/proj/a/b/c1/d1 requirements [3,3,2]
   *        /projects/proj/a/b/c1/d2 requirements [3,3,2]
   *        /projects/proj/a/b/c2    requirements [3,3]
   *        /projects/proj/a/b/c3/d1 requirements [3,3,3]
   *        /projects/proj/a/b/c3/d2 requirements [3,3,3]
   *        /projects/proj/a/b/c3/d3 requirements [3,3,3]
   *          can collapse into
   *        /projects/proj/a/b/c1     requirements [3,3]
   *        /projects/proj/a/b/c2     requirements [3,3]
   *        /projects/proj/a/b/c3     requirements [3,3]
   *          then into
   *        /projects/proj/a/b        requirements [3]
   * </pre>
   */
  class TreeProcessor extends Thread {

    private final static char DELIMITER = '|';
    private final NodeWithCount root;

    TreeProcessor() {
      root = new NodeWithCount(null, "", -1, 0, 0, false);
    }

    @Override
    public void run() {
      try {
        consumeQueue();
        waitForTesting();
        excludeUCFiles();
        minimizeTree();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    private void consumeQueue() throws InterruptedException {
      // As long as traversal is still going on, latch is not empty, keep polling for new data
      int total = 0;
      while (latch.getCount() > 0 || !results.isEmpty()) {
        RawNodeData node = results.poll();
        if (node == null) {
          Thread.sleep(100);
          continue;
        }
        total++;
        if (LOG.isDebugEnabled()) {
          node.printDebug();
        }
        root.addPath(node);
      }
      LOG.info("Collected {} cold paths", total);
    }

    private void excludeUCFiles() throws IOException {
      RemoteIterator<OpenFileEntry> ite =
          srcFs.listOpenFiles(EnumSet.of(OpenFilesIterator.OpenFilesType.ALL_OPEN_FILES),
              input.toString());
      while (ite.hasNext()) {
        root.incrRequirement(ite.next());
      }
      if (dstFs != null) {
        ite = dstFs.listOpenFiles(EnumSet.of(OpenFilesIterator.OpenFilesType.ALL_OPEN_FILES),
            input.toString());
        while (ite.hasNext()) {
          root.incrRequirement(ite.next());
        }
      }
    }

    /**
     * Recursively squashes the tree, starting from the root. Ignores directories that no longer
     * exist on source namespace.
     */
    private void minimizeTree() throws IOException, ExecutionException, InterruptedException {
      root.minimizeNode();
      Map<String, Pair<Integer, Long>> allColdDirs = getColdDirsOnSrc();

      try (FSDataOutputStream os = srcFs.create(output)) {
        for (Map.Entry<String, Pair<Integer, Long>> coldDir : allColdDirs.entrySet()) {
          os.writeBytes(String.format("%s|%d|%d%n", coldDir.getKey(), coldDir.getValue().getLeft(),
              coldDir.getValue().getRight()));
        }
      }
    }

    /**
     * Reads from the .dst_only file for paths that no longer exist on the source namespace,
     * then checks new cold dirs if they exist on source ns or not. If not, removes from the
     * list of cold dirs and writes to .dst_only file.
     * @return list of cold dirs that exist on source namespace
     */
    private Map<String, Pair<Integer, Long>> getColdDirsOnSrc()
        throws IOException, InterruptedException, ExecutionException {
      Map<String, Pair<Integer, Long>> allColdDirs =
          root.getAllLeafNodes(new HashMap<>(), new ArrayList<>());
      Set<String> toRemove = new HashSet<>();

      Path dstOnlyFilePath = new Path(output.getParent(), output.getName() + ".dst_only");
      if (srcFs.exists(dstOnlyFilePath)) {
        for (Path path : MigrationUtils.loadPathsFromDfs(srcFs, null, dstOnlyFilePath)) {
          toRemove.add(path.toUri().getPath());
        }
      }

      List<Future<Void>> futures = new ArrayList<>();
      for (String coldDir : allColdDirs.keySet()) {
        if (toRemove.contains(coldDir)) {
          continue;
        }
        futures.add(threadPool.submit(() -> {
          try {
            if (!srcFs.exists(new Path(coldDir))) {
              toRemove.add(coldDir);
            }
            return null;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }));
      }
      for (Future<Void> future : futures) {
        future.get();
      }
      try (FSDataOutputStream os = srcFs.create(dstOnlyFilePath)) {
        for (String dstOnlyDir : toRemove) {
          os.writeBytes(dstOnlyDir);
          os.write('\n');
        }
      }

      for (String dir : toRemove) {
        allColdDirs.remove(dir);
      }
      return allColdDirs;
    }
  }

  @VisibleForTesting
  public void waitForTesting() {
  }

  /**
   * Writes all files under a path recursively to an output file, one path per line.
   */
  public static void listAllFilePaths(Configuration conf, String srcNs, String pathStr,
      Path outputFile) throws IOException {
    DistributedFileSystem srcFs =
        (DistributedFileSystem) FileSystem.get(URI.create("hdfs://" + srcNs), conf);
    Path path = new Path(pathStr);
    RemoteIterator<LocatedFileStatus> ite = srcFs.listFiles(path, true);
    try (FSDataOutputStream os = srcFs.create(outputFile)) {
      while (ite.hasNext()) {
        os.writeBytes(ite.next().getPath().toUri().getPath() + "\n");
      }
    }
  }
}
