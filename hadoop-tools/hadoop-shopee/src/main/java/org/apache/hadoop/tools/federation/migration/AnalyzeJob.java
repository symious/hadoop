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

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

import org.apache.commons.lang3.tuple.Triple;
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

  private final DistributedFileSystem srcFs;
  private final Path input;
  private final Path output;
  private final long ms;
  private final int concurrency;
  /**
   * List of tuples representing cold directories, to be populated during recursive traversal.
   * The 3 values are (path, requirements, is empty dir).
   * See {@link FileStatusWithColdRequirements} to see what values are in int[] requirements.
   * <pre>
   *   E.g. a path /projects/proj/a/b/c/d with requirements [3,7,2] means
   *               /projects/proj/a/b/c need 2 children (/projects/proj/a/b/c/d being one of them)
   *                                    to be cold for the path itself to be cold
   *               /projects/proj/a/b   need 7 children to be cold for the path to be cold
   *               /projects/proj/a     need 3 children to be cold
   *               /projects/proj       is the input and will not be considered cold
   * </pre>
   * {@link Triple} are used instead of {@link FileStatusWithColdRequirements}
   * when {@link FileStatusWithColdRequirements} objects already contain all necessary info
   * to prevent OOM due to the extra memory usage by each {@link FileStatus} object,
   * which is required during the traversal step but useless during the tree creation step.
   */
  private final List<Triple<String, int[], Boolean>> results;

  public AnalyzeJob(String path, String srcNs, String threshold, Path output, int concurrency,
      Configuration conf) throws IOException {
    this.srcFs = (DistributedFileSystem) FileSystem.get(URI.create("hdfs://" + srcNs), conf);
    this.input = new Path(path);
    this.output = output;
    this.results = new ArrayList<>();
    this.ms = Long.parseLong(threshold) * 86400 * 1000;
    this.concurrency = concurrency;
    LOG.info("Analyzing path hdfs://{}{} with threshold {}, output {}", srcNs, path, threshold,
        output);
  }

  public static int handleArgs(List<String> argsList, Configuration conf) throws IOException {
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
    String concurrencyStr = StringUtils.popOptionWithArgument("-output", argsList);
    if (concurrencyStr != null) {
      concurrency = Integer.parseInt(concurrencyStr);
    }
    AnalyzeJob job = new AnalyzeJob(path, src, threshold, new Path(outputFile), concurrency, conf);
    job.execute();
    return 0;
  }

  /**
   * Container class with {@link FileStatus} and the number of cold dirs
   * of the same level to consider parent also a cold dir.
   * <br>
   * E.g. /base/a, /base/b, /base/file. /base/file is checked to be cold when
   * getListing(/base) is called. If /base/a and /base/b are both cold, then /base
   * can be considered cold. The augmented number stored by /base/a and /base/b is 2. After
   * the recursive part involving getListing calls, reconstruct a filesystem tree and backtrack
   * to create the minimal set of bottom level cold directories.
   */
  static class FileStatusWithColdRequirements {
    private final FileStatus status;
    /** All requirements down to this inode */
    private final int[] requirements;

    FileStatusWithColdRequirements(FileStatus status, int[] requirements) {
      this.status = status;
      this.requirements = requirements;
    }
  }

  /**
   * Recursively traverses through a subtree starting at {@link AnalyzeJob#input},
   * populates {@link AnalyzeJob#results}. Once the traversal is done, reconstructs a filesystem
   * tree containing only dirs in {@link AnalyzeJob#results} into a tree of {@link NodeWithCount}.
   * Then collapses as many {@link NodeWithCount} as possible into parent nodes.
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
  class AnalyzeSubroutine extends RecursiveAction {

    private final FileStatusWithColdRequirements base;

    AnalyzeSubroutine(FileStatusWithColdRequirements base) {
      this.base = base;
    }

    @Override
    protected void compute() {
      FileStatus[] statuses;
      try {
        statuses = srcFs.listStatus(base.status.getPath());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      // Count all the cold files first
      int coldDirsNeeded = statuses.length;
      for (FileStatus status : statuses) {
        if (status.isFile() && Time.now() - base.status.getModificationTime() > ms
            && Time.now() - status.getModificationTime() > ms) {
          coldDirsNeeded--;
        }
      }

      // If everything is cold files, the dir is cold dir
      if (coldDirsNeeded == 0) {
        synchronized (results) {
          String pathStr = base.status.getPath().toUri().getPath();
          results.add(Triple.of(pathStr, base.requirements, statuses.length == 0));
        }
        return;
      }

      List<AnalyzeSubroutine> subtasks = new ArrayList<>();
      for (FileStatus status : statuses) {
        if (!status.isFile()) {
          int[] currentRequirements = new int[base.requirements.length + 1];
          System.arraycopy(base.requirements, 0, currentRequirements, 0, base.requirements.length);
          currentRequirements[base.requirements.length] = coldDirsNeeded;
          subtasks.add(new AnalyzeSubroutine(
              new FileStatusWithColdRequirements(status, currentRequirements)));
        }
      }
      invokeAll(subtasks);
    }
  }

  public void execute() throws IOException {
    ForkJoinPool p = new ForkJoinPool(concurrency);
    RecursiveAction task = new AnalyzeSubroutine(
        new FileStatusWithColdRequirements(srcFs.getFileStatus(input), new int[0]));
    p.execute(task);
    task.join();
    p.shutdown();

    LOG.info("Collected {} cold paths", results.size());
    minimizeBottomLevelDirs();
  }

  /**
   * Augmented node with an integer value indicating how many of its children nodes need to be
   * cold for the node itself to be cold. Do note that this value is different from
   * {@link FileStatusWithColdRequirements} where the parent's requirement is stored
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
    NodeWithCount parent;
    Map<String, NodeWithCount> children = null;
    boolean empty;

    NodeWithCount(NodeWithCount parent, String name, int count, boolean empty) {
      this.parent = parent;
      this.name = name;
      this.count = count;
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
        children.clear();
        children = null;
        count = 0;
      }
    }

    /**
     * Adds a path to this node. Should only be called from the root "/" node.
     */
    void addPath(String path, int[] requirements, boolean empty) {
      String[] components = path.split("/");

      NodeWithCount cur = this;
      for (int i = 1; i < components.length; i++) {
        final String component = components[i];
        NodeWithCount finalCur = cur;
        if (cur.children == null) {
          cur.children = new HashMap<>();
        }
        cur = finalCur.children.compute(component,
            (k, v) -> v == null ? new NodeWithCount(finalCur, component, -1, false) : v);
      }
      // Update leaf node only
      cur.empty = empty;
      cur.count = 0;

      // Backtrack to fill the counts
      cur = cur.parent;
      for (int i = requirements.length - 1; i >= 0; i--) {
        cur.count = requirements[i];
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
        if (!cur.children.containsKey(component)) {
          return;
        }
        cur = cur.children.get(component);
      }
      // If can fully traverse the tree, increment requirement if not leaf node, else drop the node
      if (children == null && count == 0) {
        cur.parent.children.remove(cur.name);
        cur.parent = null;
      } else {
        cur.count++;
      }
    }

    StringBuilder getAllLeafNodes(StringBuilder result, List<String> components)
        throws IOException {
      // Do not include empty dirs
      if (empty) {
        return result;
      }
      if (count == 0) {
        result.append("/");
        result.append(String.join("/", components));
        result.append("\n");
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

  private void minimizeBottomLevelDirs() throws IOException {
    // Reconstruct tree
    NodeWithCount root = new NodeWithCount(null, "", -1, false);
    for (Triple<String, int[], Boolean> path : results) {
      root.addPath(path.getLeft(), path.getMiddle(), path.getRight());
    }
    RemoteIterator<OpenFileEntry> ite =
        srcFs.listOpenFiles(EnumSet.of(OpenFilesIterator.OpenFilesType.ALL_OPEN_FILES),
            input.toString());
    while (ite.hasNext()) {
      root.incrRequirement(ite.next());
    }
    // Recursively squash the tree, starting from root
    root.minimizeNode();
    try (FSDataOutputStream os = srcFs.create(output)) {
      os.writeBytes(root.getAllLeafNodes(new StringBuilder(), new ArrayList<>()).toString());
    }
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
