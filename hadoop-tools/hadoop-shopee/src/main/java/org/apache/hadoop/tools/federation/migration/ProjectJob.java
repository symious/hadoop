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
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One of the supported commands for {@link NSMigrationTool}.
 * One button solution that takes in a /projects/project style path and runs everything
 * from start to finish. Supports resuming.
 */
public class ProjectJob {
  private static final Logger LOG = LoggerFactory.getLogger(ProjectJob.class);

  final static public Path BASE_PATH = new Path("/tmp/__MIGRATION_PROJECT_JOBS/");
  private final DistributedFileSystem srcFs;

  private final Path path;
  private final String projectName;
  private final String srcNs;
  private final String dstNs;
  private final String fedNs;
  private final String routerAddr;
  private final int listingThreads;
  private final int workerThreads;
  private final long stopThreshold;
  private final int coldThreshold;
  private final boolean hot;
  private final Configuration conf;
  private int emptyColdCycle;

  private final Path inputPathsFilePath;
  private final Path donePathsFilePath;
  private final Path coldContextFilePath;
  private final Path hotContextFilePath;

  public ProjectJob(String path, String projectName, String src, String dst, String fed,
      String routerAddr, int listingThreads, int workerThreads, long stopThreshold,
      int coldThreshold, boolean hotMode, Configuration conf) throws IOException {
    this.path = new Path(path);
    if (projectName == null) {
      this.projectName = path.split("/")[2];
    } else {
      this.projectName = projectName;
    }
    this.srcNs = src;
    this.dstNs = dst;
    this.fedNs = fed;
    this.routerAddr = routerAddr;
    this.listingThreads = listingThreads;
    this.workerThreads = workerThreads;
    this.stopThreshold = stopThreshold;
    this.coldThreshold = coldThreshold;
    this.hot = hotMode;
    this.conf = conf;

    this.srcFs = (DistributedFileSystem) FileSystem.get(URI.create("hdfs://" + srcNs), conf);

    this.inputPathsFilePath = new Path(BASE_PATH, this.projectName + "_input.txt");
    this.donePathsFilePath = new Path(BASE_PATH, this.projectName + "_done.txt");

    this.coldContextFilePath = new Path(BASE_PATH, "._MIGRATION_COLD_" + projectName);
    this.hotContextFilePath = new Path(BASE_PATH, "._MIGRATION_HOT_" + projectName);
  }

  public static int handleArgs(List<String> argsList, Configuration conf) throws Exception {
    // Project to migrate
    String path = StringUtils.popOptionWithArgument("-path", argsList);
    if (path == null) {
      System.err.println("A path of format /projects/project must be provided via -path option.");
      return -1;
    }
    while (path.endsWith("/")) {
      path = path.substring(0, path.length() - 1);
    }
    String projectName = StringUtils.popOptionWithArgument("-project", argsList);

    // Namespaces
    String src = StringUtils.popOptionWithArgument("-src", argsList);
    String dst = StringUtils.popOptionWithArgument("-dst", argsList);
    String fed = StringUtils.popOptionWithArgument("-fed", argsList);
    if (src == null || dst == null || fed == null) {
      System.err.println("-src, -dst, -fed options are required.");
      return -1;
    }

    // Router to create/update/delete mount points
    String routerAddr = StringUtils.popOptionWithArgument("-router", argsList);
    if (routerAddr == null) {
      System.err.println("A router must be defined via option -router.");
      return -1;
    }

    // How many threads to analyze paths?
    String listingThreadsStr = StringUtils.popOptionWithArgument("-listingThreads", argsList);
    int listingThreads = listingThreadsStr == null ? 64 : Integer.parseInt(listingThreadsStr);
    // How many jobs to run in parallel in batch mode?
    String workerThreadsStr = StringUtils.popOptionWithArgument("-workerThreads", argsList);
    int workerThreads = workerThreadsStr == null ? 16 : Integer.parseInt(workerThreadsStr);

    // How many days to consider data cold?
    String coldThresholdStr = StringUtils.popOptionWithArgument("-coldThreshold", argsList);
    int coldThreshold = coldThresholdStr == null ? 10 : Integer.parseInt(coldThresholdStr);

    // Run hot mode?
    boolean hotMode = StringUtils.popOption("-hot", argsList);

    // How many files + subdirs left before stopping cycling cold mode?
    String stopThresholdStr = StringUtils.popOptionWithArgument("-stop", argsList);
    long stopThreshold = stopThresholdStr == null ? 0 : Long.parseLong(stopThresholdStr);

    if (stopThreshold != 0 && hotMode) {
      System.err.println("-stop or -hot not allowed at the same time.");
      return -1;
    }

    ProjectJob job =
        new ProjectJob(path, projectName, src, dst, fed, routerAddr, listingThreads, workerThreads,
            stopThreshold, coldThreshold, hotMode, conf);
    job.execute();
    return 0;
  }

  public void execute() throws Exception {
    if (stopThreshold != 0) {
      int cycle = 1;
      LOG.info("Starting cold cycle {}", cycle);
      // Ensure at least 1 cold cycle before checking for stop condition
      coldCycle();
      while (!shouldStop()) {
        // Rate limit cycles
        Thread.sleep(60000);
        cycle++;
        LOG.info("Starting cold cycle {}", cycle);
        coldCycle();
      }
      return;
    }

    if (hot) {
      hotRun();
    }
  }

  private boolean shouldStop() throws IOException {
    // Short circuit and resume previous run that already progressed to cold phase if possible
    if (srcFs.exists(coldContextFilePath)) {
      return false;
    }
    if (emptyColdCycle > 1) {
      LOG.warn("{} analyze jobs in a row found no cold dirs.", emptyColdCycle);
    }
    DistributedFileSystem srcFs =
        (DistributedFileSystem) FileSystem.get(URI.create("hdfs://" + this.srcNs), conf);
    ContentSummary content = srcFs.getContentSummary(path);
    if (content.getFileCount() == 0) {
      LOG.info("No files left to migrate. Stopping cold cycles.");
      return true;
    }
    LOG.info("Total content count {} (files={},dirs={}) for path {}",
        content.getFileAndDirectoryCount(), content.getFileCount(), content.getDirectoryCount(),
        path);
    return content.getFileAndDirectoryCount() <= stopThreshold;
  }

  private void coldCycle() throws Exception {
    startAnalyzeJobIfNecessary();
    startBatchJob();
    cleanPathsFiles();
  }

  /**
   * Checks input/output files, starts a new analyze job if there is no input file, or output file
   * already contains all paths in input file. Skip entirely if there is a previous job already
   * in COLD or HOT phase.
   */
  private void startAnalyzeJobIfNecessary() throws IOException {
    // Resume a previous job already progressed to cold or hot phase.
    if (srcFs.exists(coldContextFilePath)) {
      LOG.info("Resuming a previous run from COLD phase.");
      return;
    }
    if (!srcFs.exists(inputPathsFilePath)) {
      LOG.info("No input file found, starting a new analyze job.");
      startAnalyzeJob();
      return;
    }
    Set<Path> allPaths = MigrationUtils.loadPathsFromDfs(srcFs, path, inputPathsFilePath);
    if (allPaths.contains(path) && allPaths.size() == 1) {
      LOG.info("Input file empty, starting a new analyze job.");
      startAnalyzeJob();
      return;
    }
    Set<Path> donePaths = MigrationUtils.loadPathsFromDfs(srcFs, path, donePathsFilePath);
    // Previous batch job done, start a new analysis, clear current paths
    if (!donePaths.isEmpty() && donePaths.containsAll(allPaths)) {
      LOG.info("All current dirs migrated, starting a new analyze job for new cold dirs");
      cleanPathsFiles();
      startAnalyzeJob();
    }
  }

  private void startAnalyzeJob() throws IOException {
    AnalyzeJob job =
        new AnalyzeJob(path.toString(), srcNs, dstNs, fedNs, String.valueOf(coldThreshold),
            inputPathsFilePath, listingThreads, conf);
    job.execute();
  }

  /**
   * Reads the input + output files, start batch jobs. Skip if already in hot phase.
   */
  private void startBatchJob() throws Exception {
    if (srcFs.exists(hotContextFilePath)) {
      LOG.info("Hot context file detected, either resume the hot migration or "
          + "make sure it's finished and delete the context file then retry cold migration.");
      System.exit(0);
    }
    srcFs.create(coldContextFilePath, true).close();
    Set<Path> allPaths = MigrationUtils.loadPathsFromDfs(srcFs, path, inputPathsFilePath);
    Set<Path> donePaths = MigrationUtils.loadPathsFromDfs(srcFs, path, donePathsFilePath);
    allPaths.removeAll(donePaths);
    if (allPaths.isEmpty()) {
      LOG.info("There's nothing to migrate.");
      emptyColdCycle++;
      srcFs.delete(coldContextFilePath);
      return;
    }
    // Reset emptyColdCycle if some data to migration is found
    emptyColdCycle = 0;
    MigrationJob.runBatchJob(conf, String.valueOf(workerThreads), allPaths, srcNs, dstNs,
        routerAddr, false, donePathsFilePath, false);
    srcFs.delete(coldContextFilePath);
  }

  private void hotRun() throws Exception {
    srcFs.create(hotContextFilePath, true).close();
    // Hot migration
    AnalyzeJob.listAllFilePaths(conf, srcNs, path.toString(), inputPathsFilePath);
    Set<Path> allPaths = MigrationUtils.loadPathsFromDfs(srcFs, path, inputPathsFilePath);
    MigrationJob.runBatchJob(conf, String.valueOf(workerThreads), allPaths, srcNs, dstNs,
        routerAddr, false, donePathsFilePath, true);
    cleanPathsFiles();
    srcFs.delete(hotContextFilePath);
  }

  private void cleanPathsFiles() throws IOException {
    Path backupInputPathsFilePath =
        new Path(inputPathsFilePath.getParent(), inputPathsFilePath.getName() + ".old");
    Path backupDonePathsFilePath =
        new Path(donePathsFilePath.getParent(), donePathsFilePath.getName() + ".old");
    srcFs.delete(backupInputPathsFilePath);
    srcFs.delete(backupDonePathsFilePath);
    srcFs.rename(inputPathsFilePath, backupDonePathsFilePath);
    srcFs.rename(donePathsFilePath, backupDonePathsFilePath);
  }
}
