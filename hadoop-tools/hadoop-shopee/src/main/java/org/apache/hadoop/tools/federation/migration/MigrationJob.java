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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.OpenFileEntry;
import org.apache.hadoop.hdfs.protocol.OpenFilesIterator;
import org.apache.hadoop.hdfs.server.federation.resolver.order.DestinationOrder;
import org.apache.hadoop.hdfs.server.federation.router.RouterClient;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.AddMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.GetMountTableEntriesResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.RemoveMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryRequest;
import org.apache.hadoop.hdfs.server.federation.store.protocol.UpdateMountTableEntryResponse;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.FastCopy;
import org.apache.hadoop.tools.OptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One of the supported commands for {@link NSMigrationTool}.
 * Migrates a dir from the source namespace to destination namespace. Does it through 5 steps:
 * prep, lock with mount table, distcp, unlock, clean up.
 */
public class MigrationJob {
  private static final Logger LOG = LoggerFactory.getLogger(MigrationJob.class);

  private static boolean INTERRUPT_FOR_TESTING = false;
  private static boolean skipTopTwoLevels = true;

  private static FastCopy fastCopy;

  private final DistributedFileSystem dstFs;
  private final DistributedFileSystem srcFs; // Only used for test runs
  private final RouterClient admin;
  private final Configuration conf;
  private JobStage stage;
  private JobContext context;
  /** Resuming an aborted job */
  private boolean isContinueJob;
  /** Skip listOpenFiles before disabling writes? */
  private final boolean skipOpenFiles;

  final static public Path TEMP_COPY_PATH = new Path("/tmp/__MIGRATION_FILES/");
  final static public Path RECYCLE_BIN_PATH = new Path("/migration_recycle_bin/");
  final static public FsPermission PERM_700 =
      new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE);

  public static void toggleInterruptForTesting(boolean flag) {
    INTERRUPT_FOR_TESTING = flag;
  }

  public static void toggleSkipTopTwoLevelsForTesting(boolean flag) {
    skipTopTwoLevels = flag;
  }


  public static void initializeFastCopyInstance(Configuration conf) {
    fastCopy = new FastCopy(conf);
  }

  public static int handleArgs(List<String> argsList, Configuration conf) throws Exception {
    // Path to migrate
    String path = StringUtils.popOptionWithArgument("-path", argsList);
    // Namespaces
    String src = StringUtils.popOptionWithArgument("-src", argsList);
    String dst = StringUtils.popOptionWithArgument("-dst", argsList);
    if (src == null || dst == null) {
      System.err.println("-src and -dst options are required for a new job.");
      return -1;
    }
    // Router to create/update/delete mount points
    String routerAddr = StringUtils.popOptionWithArgument("-router", argsList);
    if (routerAddr == null) {
      System.err.println("A router must be defined via option -router.");
      return -1;
    }
    // Skip waiting until no open files before migrating?
    boolean skipOpenFiles = StringUtils.popOption("-skipOpenFiles", argsList);

    // Batch mode options
    // Input file with one path per line
    String input = StringUtils.popOptionWithArgument("-batch", argsList);
    // How many paths to handle concurrently
    String concurrencyStr = StringUtils.popOptionWithArgument("-concurrency", argsList);
    // Output path with one successful path per line
    String output = StringUtils.popOptionWithArgument("-output", argsList);
    Set<Path> paths = MigrationUtils.loadPaths(new Path(path), input);

    runBatchJob(conf, concurrencyStr, paths, src, dst, routerAddr, skipOpenFiles, output, false);
    return 0;
  }

  /**
   * Spawns a threadpool to run multiple instances of {@link MigrationJob} in parallel.
   * No path is allowed to be a child or ancestor of another path.
   * @param conf            configuration to run migration
   * @param concurrencyStr  string representation of the worker thread pool size
   * @param paths           paths to migrate, have to be mutually disjunctive
   * @param src             source namespace
   * @param dst             destination namespace
   * @param routerAddr      router address for mount table operations, in IP:PORT format
   * @param skipOpenFiles   flag to skip checking for UC files, will wait until no UC files if false
   * @param output          path to output file to store migrated paths
   * @param fileMode         true to run at individual file level, in which case, input paths have
   *                        to be files
   * @throws InterruptedException
   */
  public static void runBatchJob(Configuration conf, String concurrencyStr, Set<Path> paths,
      String src, String dst, String routerAddr, boolean skipOpenFiles, String output,
      boolean fileMode)
      throws InterruptedException {
    int totalPaths = paths.size();
    if (totalPaths == 0) {
      LOG.info("There's nothing to migrate.");
      return;
    }
    LOG.info("Migrating {} paths", totalPaths);
    MigrationJob.initializeFastCopyInstance(conf);
    for (Path path : paths) {
      LOG.debug(path.toString());
    }
    int concurrency = totalPaths;
    if (concurrencyStr != null) {
      concurrency = Integer.parseInt(concurrencyStr);
      concurrency = Math.min(concurrency, totalPaths);
    }
    ExecutorService threadPool = HadoopExecutors.newFixedThreadPool(concurrency);
    Semaphore active = new Semaphore(concurrency);
    AtomicInteger done = new AtomicInteger(0);
    for (Path singlePath : paths) {
      active.acquire();
      LOG.info("Queuing {}", singlePath);
      threadPool.submit(() -> {
        try {
          MigrationJob job =
              new MigrationJob(singlePath, src, dst, conf, routerAddr, skipOpenFiles, fileMode);
          while (job.continueJob()) {
            LOG.info("Path={}, Stage {} done.", singlePath, job.stage);
          }
          if (job.stage == JobStage.FINISH) {
            LOG.info("Path={} done.", singlePath);
            if (output != null) {
              synchronized (output) {
                MigrationUtils.appendLineToFile(singlePath.toString(), output);
              }
            }
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        } finally {
          active.release();
          done.incrementAndGet();
        }
      });
    }

    int lastChecked = -1;
    while (done.get() < totalPaths) {
      Thread.sleep(10000);
      int snapshot = done.get();
      if (snapshot > lastChecked) {
        lastChecked = snapshot;
        LOG.info("{}/{} jobs done.", snapshot, totalPaths);
      }
    }
  }

  @VisibleForTesting
  public JobStage getStage() {
    return stage;
  }

  @VisibleForTesting
  public JobContext getContext() {
    return context;
  }

  public MigrationJob(Path path, String srcNs, String dstNs, Configuration conf,
      String routerAddress, boolean skipOpenFiles, boolean fileMode) throws IOException {
    this.conf = conf;
    this.dstFs = (DistributedFileSystem) FileSystem.get(URI.create("hdfs://" + dstNs), conf);
    this.srcFs = (DistributedFileSystem) FileSystem.get(URI.create("hdfs://" + srcNs), conf);
    InetSocketAddress routerSocket = NetUtils.createSocketAddr(routerAddress);
    this.admin = new RouterClient(routerSocket, conf);
    tryToLoadContext(path);
    this.context.conf = conf;
    this.context.srcNs = srcNs;
    this.context.dstNs = dstNs;
    this.context.fileMode = fileMode;
    this.skipOpenFiles = skipOpenFiles;
    LOG.info("Initialized job with context {}", context);
  }

  private void tryToLoadContext(Path path) throws IOException {
    Path contextBase = Path.mergePaths(JobContext.BASE_PATH, path);
    FileStatus[] listing = null;
    try {
      listing = dstFs.listStatus(contextBase);
    } catch (FileNotFoundException ignored) {
    }
    if (listing == null || Arrays.stream(listing)
        .noneMatch(x -> x.getPath().getName().startsWith(JobContext.CONTEXT_PREFIX))) {
      createNewJob(path);
      dstFs.mkdirs(context.contextPath);
      return;
    }

    for (int i = listing.length - 1; i >= 0; i--) {
      Path contextFile = listing[i].getPath();
      if (!contextFile.getName().startsWith(JobContext.CONTEXT_PREFIX)) {
        continue;
      }
      // __MIGRATION_1
      stage = JobStage.values()[Integer.parseInt(contextFile.getName().split("_")[3])];
      try {
        FSDataInputStream in = dstFs.open(contextFile);
        context = new JobContext(path);
        context.readFields(in);
        in.close();
        isContinueJob = true;
        break;
      } catch (IOException e) {
        // Failed to read context file, continue to the previous context file.
      }
    }
  }

  private void createNewJob(Path path) {
    stage = JobStage.PREPARE;
    context = new JobContext(path);
  }

  /**
   * Handle a stage and proceed to the next if no issue occurs.
   * <br>
   * Write context to HDFS if it's the first time the job has reached this stage, else do not
   * overwrite existing context file for this stage. Some stages might overwrite the context
   * later in {@link MigrationJob#handleStage()} but that's no business of this method.
   * <br>
   * If everything passes without any issue, proceed to the next stage and return false
   * if it's not already {@link JobStage#FINISH}, in which case return true.
   * @return true if it's the final stage and the job has finally finished.
   * @throws Exception if fail to write context to HDFS or there's an issue during handling
   */
  @VisibleForTesting
  private boolean continueJob() throws Exception {
    if (isContinueJob) {
      // Should not overwrite context file if resuming from an existing job
      isContinueJob = false;
    } else {
      writeContext();
    }

    if (!handleStage()) {
      throw new RuntimeException("Failed to execute stage " + stage.name());
    }

    return proceedToNextStage();
  }

  @VisibleForTesting
  public void writeContext() throws IOException {
    Path contextPath = new Path(context.contextPath, JobContext.CONTEXT_PREFIX + stage.stageInt);
    // Overwrite stage context
    try (FSDataOutputStream os = dstFs.create(contextPath, true)) {
      context.write(os);
    }
    LOG.info("Saved context for stage {} to {}", stage.name(), contextPath);
  }

  @VisibleForTesting
  public boolean proceedToNextStage() {
    if (stage == JobStage.FINISH) {
      return false;
    } else {
      // Proceeds to the next stage
      stage = JobStage.values()[stage.stageInt + 1];
      return true;
    }
  }

  /**
   * Handle the stage.
   * @return true if the stage was handled properly without any issue
   * @throws Exception if any issue occurs during handling.
   */
  @VisibleForTesting
  public boolean handleStage() throws Exception {
    switch (stage) {
      case PREPARE:
        return prepareJob();
      case MOUNT:
        return disableWrite();
      case COPY:
        return migratePath();
      case POST_COPY:
        return cleanup();
      case FINISH:
        return removeMountAndCleanContext();
      default:
        LOG.error("Unknown stage {}", stage.name());
        throw new IllegalArgumentException("Unknown stage " + stage.name());
    }
  }

  private boolean prepareJob() throws IOException {
    try {
      if (context.fileMode) {
        srcFs.getFileStatus(context.path);
      } else {
        FileStatus[] listing = srcFs.listStatus(context.path);
        if (listing.length == 0) {
          LOG.info("Nothing to migrate from {} on the source namespace {}. Skipping migration.",
              context.path, context.srcNs);
          // Progress directly to after COPY, before FINISH stage
          context.prepareStageFailed = true;
          stage = JobStage.POST_COPY;
          return true;
        }
      }
    } catch (FileNotFoundException e) {
      LOG.info("{} does not exist on the source namespace {}. Skipping migration.", context.path,
          context.srcNs);
      // Progress directly to after COPY, before FINISH stage
      context.prepareStageFailed = true;
      stage = JobStage.POST_COPY;
      return true;
    }
    GetMountTableEntriesRequest request = GetMountTableEntriesRequest.newInstance(context.pathStr);
    GetMountTableEntriesResponse response =
        admin.getMountTableManager().getMountTableEntries(request);
    List<MountTable> mountPoints = response.getEntries();
    if (mountPoints.isEmpty()) {
      // Valid job. Create all necessary ancestor directories.
      // Can ignore the path itself because it'll be created with the right attributes by distcp.
      CreateTopDirJob.createDirsWithPermission(srcFs, dstFs, context.path.getParent(), !skipTopTwoLevels);
      return true;
    }
    LOG.error("Cannot initiate migration on existing mount point {}.", context.path);
    context.prepareStageFailed = true;
    stage = JobStage.POST_COPY;
    return true;
  }

  private boolean disableWrite() throws IOException, InterruptedException {
    if (!skipOpenFiles) {
      while (true) {
        RemoteIterator<OpenFileEntry> openFiles =
            srcFs.listOpenFiles(EnumSet.of(OpenFilesIterator.OpenFilesType.ALL_OPEN_FILES),
                context.pathStr);
        if (openFiles.hasNext()) {
          LOG.info("There is at least one open file {}, waiting until there is none...",
              openFiles.next().getFilePath());
          Thread.sleep(2000);
        } else {
          break;
        }
      }
    }
    Map<String, String> destMap = new LinkedHashMap<>();
    destMap.put(context.srcNs, context.pathStr);
    MountTable newEntry = MountTable.newInstance(context.pathStr, destMap);
    newEntry.setReadOnly(true);
    newEntry.setDestOrder(DestinationOrder.HASH);

    AddMountTableEntryRequest request = AddMountTableEntryRequest.newInstance(newEntry);
    AddMountTableEntryResponse addResponse =
        admin.getMountTableManager().addMountTableEntry(request);

    interruptForTesting();
    boolean added = addResponse.getStatus();
    if (!added) {
      LOG.error("Failed to mount {} to {}", context.path, newEntry);
    }
    return added;
  }

  private boolean migratePath() throws Exception {
    if (context.fileMode) {
      return fileMigrate();
    } else {
      return migrateWithDistCp();
    }
  }

  private boolean fileMigrate() throws Exception {
    // Create a unique path for the temp destination
    Path tempPath = Path.mergePaths(TEMP_COPY_PATH, context.path);
    dstFs.delete(tempPath);
    dstFs.mkdirs(tempPath.getParent(), PERM_700);

    assert fastCopy != null;
    FastCopy.CopyResult result = fastCopy.copy(context.pathStr, tempPath.toString(), srcFs, dstFs);

    if (result != FastCopy.CopyResult.SUCCESS) {
      return false;
    }

    return dstFs.rename(tempPath, context.path);
  }

  private boolean migrateWithDistCp() throws Exception {
    String path = context.pathStr;
    Path pathObj = new Path(path);
    String src = String.format("hdfs://%s%s", context.srcNs, pathObj);
    String dst = String.format("hdfs://%s%s", context.dstNs, pathObj.getParent().toString());
    String[] args = new String[] { "-fastCopyEnable", "-prbugpcaxte", src, dst };

    Configuration config = new Configuration(conf);
    if (context.jobID.isEmpty()) {
      DistCp distCp = new DistCp(config, OptionsParser.parse(args));
      Job job = distCp.createAndSubmitJob();
      context.jobID = job.getJobID().toString();
      writeContext();
      interruptForTesting();
      return job.waitForCompletion(false);
    } else {
      JobClient client = new JobClient(config);
      RunningJob job = client.getJob(JobID.forName(context.jobID));
      if (job == null) {
        FileStatus[] listing = null;
        try {
          listing = srcFs.listStatus(context.path);
        } catch (FileNotFoundException ignored) {
        }
        if (listing == null && listing.length == 0) {
          LOG.info("Cannot find past job {}, source ns is empty, job succeeded.", context.jobID);
          return true;
        } else {
          // Submit a new one
          LOG.info("Cannot find past job {}, source ns is not empty, submitting a new job...",
              context.jobID);
          DistCp distCp = new DistCp(config, OptionsParser.parse(args));
          Job newJob = distCp.createAndSubmitJob();
          context.jobID = newJob.getJobID().toString();
          writeContext();
          interruptForTesting();
          return newJob.waitForCompletion(false);
        }
      } else {
        if (job.isComplete() && !job.isSuccessful()) {
          // Submit a new one
          LOG.info("Old job {} failed, submitting another one...", context.jobID);
          DistCp distCp = new DistCp(config, OptionsParser.parse(args));
          Job newJob = distCp.createAndSubmitJob();
          context.jobID = newJob.getJobID().toString();
          writeContext();
          interruptForTesting();
          return newJob.waitForCompletion(false);
        }
        job.waitForCompletion();
        return job.isSuccessful();
      }
    }
  }

  private boolean cleanup() throws IOException {
    if (!dstFs.exists(context.path)) {
      LOG.error("Path {} not found in destination namespace {} after migration!", context.path,
          context.dstNs);
      return false;
    }
    if (!context.fileMode) {
      // Check all subdirs exist in destination
      FileStatus[] srcListing = srcFs.listStatus(context.path);
      FileStatus[] dstListing = dstFs.listStatus(context.path);
      Set<String> dstListingSet =
          Arrays.stream(dstListing).map(x -> x.getPath().getName()).collect(Collectors.toSet());
      for (FileStatus status : srcListing) {
        if (!dstListingSet.contains(status.getPath().getName())) {
          LOG.error("Path {} not found in destination namespace {} after migration!",
              status.getPath(), context.dstNs);
          return false;
        }
      }
      // Random file check
      List<String> pathsToCheck = new ArrayList<>();
      RemoteIterator<LocatedFileStatus> ite = srcFs.listFiles(context.path, true);
      while (ite.hasNext() && pathsToCheck.size() < 10) {
        pathsToCheck.add(ite.next().getPath().toUri().getPath());
      }
      for (String pathToCheck : pathsToCheck) {
        if (!dstFs.exists(new Path(pathToCheck))) {
          LOG.error("Path {} not found in destination namespace {} after migration!", pathToCheck,
              context.dstNs);
          return false;
        }
      }
    }
    GetMountTableEntriesRequest getRequest =
        GetMountTableEntriesRequest.newInstance(context.pathStr);
    GetMountTableEntriesResponse getResponse =
        admin.getMountTableManager().getMountTableEntries(getRequest);
    MountTable existing = getResponse.getEntries().get(0);

    Map<String, String> destMap = new LinkedHashMap<>();
    destMap.put(context.dstNs, context.pathStr);
    MountTable entry = MountTable.newInstance(context.pathStr, destMap);
    existing.setDestinations(entry.getDestinations());
    UpdateMountTableEntryRequest updateRequest =
        UpdateMountTableEntryRequest.newInstance(existing);
    UpdateMountTableEntryResponse updateResponse =
        admin.getMountTableManager().updateMountTableEntry(updateRequest);

    interruptForTesting();
    boolean updated = updateResponse.getStatus();
    if (!updated) {
      LOG.error("Failed to update mount {} to readonly", existing);
      return false;
    }
    return moveToMigrationRecycleBin();
  }

  private boolean moveToMigrationRecycleBin() throws IOException {
    Path destination = Path.mergePaths(RECYCLE_BIN_PATH, context.path);
    srcFs.mkdirs(destination.getParent(), PERM_700);
    boolean result = srcFs.rename(context.path, destination);
    if (!result) {
      LOG.info("Failed to move {} to recycle bin", context.path);
    }
    return result;
  }

  private boolean removeMountAndCleanContext() throws IOException {
    if (!context.prepareStageFailed) {
      RemoveMountTableEntryRequest request =
          RemoveMountTableEntryRequest.newInstance(context.pathStr);
      RemoveMountTableEntryResponse response =
          admin.getMountTableManager().removeMountTableEntry(request);
      interruptForTesting();
      boolean removed = response.getStatus();
      if (!removed) {
        GetMountTableEntriesRequest getRequest =
            GetMountTableEntriesRequest.newInstance(context.pathStr);
        GetMountTableEntriesResponse getResponse =
            admin.getMountTableManager().getMountTableEntries(getRequest);
        if (!getResponse.getEntries().isEmpty()) {
          LOG.error("Failed to remove temp mount point for path {}", context.pathStr);
          return false;
        }
      }
    }

    // Delete all context files
    dstFs.delete(context.contextPath, true);
    return true;
  }

  @VisibleForTesting
  public void interruptForTesting() {
    if (INTERRUPT_FOR_TESTING) {
      throw new RuntimeException("Interrupted stage for testing.");
    }
  }

  /**
   * Indicates which stage a job is at (before the stage is carried out, e.g. MOUNT stage means
   * the mount point is not added yet).
   */
  public enum JobStage {
    // Basic checks like empty dir, existing mount points
    PREPARE(0), // Disable write by mounting path to source ns under readonly
    MOUNT(1), // Migrate data using distcp
    COPY(2), // Switch temp mount point to destination ns, remove data from source ns
    POST_COPY(3), // Remove temp mount point, remove context files
    FINISH(4), // POST_FINISH is only used as a placeholder for unit tests
    POST_FINISH(5);

    final int stageInt;

    JobStage(int v) {
      stageInt = v;
    }

    @VisibleForTesting
    public int getStageInt() {
      return stageInt;
    }
  }

  /**
   * Settings pertained to the job. Will be stored on the destination namespace, under
   * {@value BASE_PATH_STR} and have an integer suffix indicating which stage a job is at.
   * <br>
   * Context files won't participate in migration since they are already on the
   * destination namespace.
   */
  public static class JobContext implements Writable {
    final static public String BASE_PATH_STR = "/tmp/__MIGRATION/";
    final static public Path BASE_PATH = new Path(BASE_PATH_STR);
    final static public String CONTEXT_PREFIX = "__MIGRATION_";

    /** Paths to work on */
    final Path path;
    final String pathStr;
    /** Base dir to write context files to */
    final Path contextPath;

    /** Configuration used for this job */
    Configuration conf;
    /** Source namespace */
    String srcNs = "";
    /** Destination namespace */
    String dstNs = "";
    /** ID for existing DistCp job */
    String jobID = "";
    /** Failed on prepare stage? */
    public boolean prepareStageFailed = false;
    /** This job is for a single file?*/
    public boolean fileMode;

    JobContext(Path path) {
      this.path = path;
      this.pathStr = path.toString();
      contextPath = Path.mergePaths(JobContext.BASE_PATH, path);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      conf.write(out);
      Text.writeString(out, srcNs);
      Text.writeString(out, dstNs);
      Text.writeString(out, jobID);
      out.writeBoolean(prepareStageFailed);
      out.writeBoolean(fileMode);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      conf = new Configuration(false);
      conf.readFields(in);
      srcNs = Text.readString(in);
      dstNs = Text.readString(in);
      jobID = Text.readString(in);
      prepareStageFailed = in.readBoolean();
      fileMode = in.readBoolean();
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof JobContext)) {
        return false;
      }
      JobContext other = (JobContext) obj;
      return conf.toString().equals(other.conf.toString())
          && path.equals(other.path)
          && pathStr.equals(other.pathStr)
          && srcNs.equals(other.srcNs)
          && dstNs.equals(other.dstNs)
          && jobID.equals(other.jobID)
          && prepareStageFailed == other.prepareStageFailed
          && fileMode == other.fileMode;
    }

    @Override
    public String toString() {
      return "path=" + path + ",contextPath=" + contextPath + ",srcNs=" + srcNs + ",dstNs=" + dstNs
          + ",jobID=" + jobID;
    }

    @VisibleForTesting
    public Configuration getConf() {
      return conf;
    }

    @VisibleForTesting
    public String getJobID() {
      return jobID;
    }

    @VisibleForTesting
    public Path getContextPath() {
      return contextPath;
    }
  }
}
