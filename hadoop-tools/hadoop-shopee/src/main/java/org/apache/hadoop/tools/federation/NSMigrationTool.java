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
package org.apache.hadoop.tools.federation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
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
import org.apache.hadoop.tools.OptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NSMigrationTool extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory.getLogger(NSMigrationTool.class);
  private static boolean INTERRUPT_FOR_TESTING = false;

  public static void toggleInterruptForTesting(boolean flag) {
    INTERRUPT_FOR_TESTING = flag;
  }

  /**
   * Settings pertained to the job. Will be stored on the destination namespace, under
   * the destination path with filename prefix {@value FILE_PREFIX} and an integer suffix
   * indicating which stage a job is at.
   * <br>
   * Context files won't participate in migration since they are already on the
   * destination namespace.
   */
  public static class JobContext implements Writable {
    final static public String BASE_PATH = "/tmp/__MIGRATION/";

    /** Path to work on */
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
    /** Use dir permission instead of router mount. Only use this option for testing. */
    public boolean dirLock;

    JobContext(Path path) {
      this.path = path;
      this.pathStr = path.toString();
      contextPath = new Path(JobContext.BASE_PATH + path);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      conf.write(out);
      Text.writeString(out, srcNs);
      Text.writeString(out, dstNs);
      Text.writeString(out, jobID);
      out.writeBoolean(dirLock);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      conf = new Configuration(false);
      conf.readFields(in);
      srcNs = Text.readString(in);
      dstNs = Text.readString(in);
      jobID = Text.readString(in);
      dirLock = in.readBoolean();
    }
    
    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof JobContext)) {
        return false;
      }
      JobContext other = (JobContext) obj;
      return conf.toString().equals(other.conf.toString()) &&
        path.equals(other.path) &&
        pathStr.equals(other.pathStr) &&
        srcNs.equals(other.srcNs) &&
        dstNs.equals(other.dstNs) &&
        jobID.equals(other.jobID) &&
        dirLock == other.dirLock;
    }
  }

  /**
   * Indicates which stage a job is at (before the stage is carried out, e.g. MOUNT stage means
   * the mount point is not added yet).
   */
  public enum JobStage {
    // Basic checks like empty dir, existing mount points
    PREPARE(0),
    // Disable write by mounting path to source ns under readonly
    MOUNT(1),
    // Migrate data using distcp
    COPY(2),
    // Switch temp mount point to destination ns, remove data from source ns
    POST_COPY(3),
    // Remove temp mount point, remove context files
    FINISH(4),
    // POST_FINISH is only used as a placeholder for unit tests
    POST_FINISH(5);

    final int stageInt;

    JobStage(int v) {
      stageInt = v;
    }
  }

  public static class MigrationJob {
    private final DistributedFileSystem dstFs;
    private final DistributedFileSystem srcFs; // Only used for test runs
    private final RouterClient admin;
    private final Configuration conf;
    private JobStage stage;
    private JobContext context;
    /** Resuming an aborted job */
    private boolean isContinueJob;

    @VisibleForTesting
    public JobStage getStage() {
      return stage;
    }

    @VisibleForTesting
    public JobContext getContext() {
      return context;
    }

    MigrationJob(Path path, String srcNs, String dstNs, Configuration conf, String routerAddress,
        boolean dirLock) throws IOException {
      this.conf = conf;
      this.dstFs = (DistributedFileSystem) FileSystem.get(URI.create("hdfs://" + dstNs), conf);
      this.srcFs = (DistributedFileSystem) FileSystem.get(URI.create("hdfs://" + srcNs), conf);
      InetSocketAddress routerSocket = NetUtils.createSocketAddr(routerAddress);
      this.admin = new RouterClient(routerSocket, conf);
      tryToLoadContext(path);
      this.context.conf = conf;
      this.context.srcNs = srcNs;
      this.context.dstNs = dstNs;
      this.context.dirLock = dirLock;
    }

    private void tryToLoadContext(Path path) throws IOException {
      Path contextBase = new Path(JobContext.BASE_PATH + path.toString());
      FileStatus[] listing = null;
      try {
        listing = dstFs.listStatus(contextBase);
      } catch (FileNotFoundException ignored) {
      }
      if (listing == null || listing.length == 0) {
        createNewJob(path);
        dstFs.mkdirs(context.contextPath);
        return;
      }

      for (int i = listing.length - 1; i >= 0; i--) {
        Path contextFile = listing[i].getPath();
        stage = JobStage.values()[Integer.parseInt(contextFile.getName())];
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
      Path contextPath = new Path(context.contextPath, String.valueOf(stage.stageInt));
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
      DistributedFileSystem srcFs =
          (DistributedFileSystem) FileSystem.get(URI.create("hdfs://" + context.srcNs), conf);
      try {
        FileStatus[] listing = srcFs.listStatus(context.path);
        if (listing.length == 0) {
          LOG.info("Nothing to migrate from {} on the source namespace {}. Skipping migration.",
              context.path, context.srcNs);
          // Progress directly to after COPY, before FINISH stage
          stage = JobStage.POST_COPY;
          return true;
        }
      } catch (FileNotFoundException e) {
        LOG.info("{} does not exist on the source namespace {}. Skipping migration.",
            context.path, context.srcNs);
        // Progress directly to after COPY, before FINISH stage
        stage = JobStage.POST_COPY;
        return true;
      }
      GetMountTableEntriesRequest request =
          GetMountTableEntriesRequest.newInstance(context.pathStr);
      GetMountTableEntriesResponse response =
          admin.getMountTableManager().getMountTableEntries(request);
      List<MountTable> mountPoints = response.getEntries();
      if (mountPoints.isEmpty()) {
        return true;
      }
      LOG.error("Cannot initiate migration on existing mount point {}.", context.path);
      stage = JobStage.POST_COPY;
      return true;
    }

    private boolean disableWrite() throws IOException {
      if (context.dirLock) {
        // Lock with dir permission instead of mount points, for testing only
        srcFs.setPermission(context.path, FsPermission.createImmutable((short) 0555));
        return true;
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
      String path = context.pathStr;
      Path pathObj = new Path(path);
      String src = String.format("hdfs://%s%s", context.srcNs, pathObj);
      String dst = String.format("hdfs://%s%s", context.dstNs, pathObj.getParent().toString());
      String[] args = { "-fastCopyEnable", "-existIgnorePreserve", "-prbugpcaxt", src, dst };

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
      if (context.dirLock) {
        srcFs.setPermission(context.path, FsPermission.createImmutable((short) 0700));
        dstFs.setPermission(context.path, FsPermission.createImmutable((short) 0555));
      } else {
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
      }
      return Trash.moveToAppropriateTrash(srcFs, context.path, context.conf);
    }

    private boolean removeMountAndCleanContext() throws IOException {
      if (context.dirLock) {
        dstFs.setPermission(context.path, FsPermission.createImmutable((short) 0700));
      } else {
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
  }

  public NSMigrationTool(Configuration conf) {
    super(conf);
  }

  public static void main(String[] argv) {
    Configuration conf = new Configuration();
    NSMigrationTool tool = new NSMigrationTool(conf);
    int exitCode;
    try {
      exitCode = ToolRunner.run(tool, argv);
    } catch (Exception e) {
      LOG.error("Failed to run MigrationTool", e);
      exitCode = -1;
    }
    System.exit(exitCode);
  }

  @Override
  public int run(String[] args) throws Exception {
    List<String> argsList = new LinkedList<>(Arrays.asList(args));
    String path = StringUtils.popOptionWithArgument("-path", argsList);
    if (path == null) {
      System.err.println("-path option is required.");
      return -1;
    }
    MigrationJob job;
    String src = StringUtils.popOptionWithArgument("-src", argsList);
    String dst = StringUtils.popOptionWithArgument("-dst", argsList);
    if (src == null || dst == null) {
      System.err.println("-src and -dst options are required for a new job.");
      return -1;
    }
    String routerAddress = StringUtils.popOptionWithArgument("-router", argsList);
    if (routerAddress == null) {
      System.err.println("A router must be used via option -router.");
      return -1;
    }
    // This option is used for client testing without needing to deploy new routers.
    boolean dirLock = StringUtils.popOption("-dirLock", argsList);
    job = new MigrationJob(new Path(path), src, dst, getConf(), routerAddress, dirLock);
    while (job.continueJob()) {
      System.out.println("Stage " + job.stage + " done.");
    }
    return 0;
  }
}
