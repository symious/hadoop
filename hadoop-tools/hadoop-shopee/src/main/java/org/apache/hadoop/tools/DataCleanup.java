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
package org.apache.hadoop.tools;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataCleanup extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory.getLogger(DataCleanup.class);
  private static final DateFormat DLM_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
  private static final String DEFAULT_TEMP_KEY = "dfs.tools.data.cleanup.temp.path";
  private static final String DEFAULT_TEMP_PATH = "/projects/dlm/hdfs/prod/datacleanup_temp";

  private CleanupAction action;
  private boolean verbose = false;
  private int concurrency;
  private Set<Path> paths;
  private FileSystem fs;
  private Path tempPath;
  private Path datePath;

  enum CleanupAction {
    TEMP, TRASH, DELETE
  }

  public DataCleanup(Configuration conf) {
    super(conf);
  }

  @Override
  public int run(String[] args) throws Exception {
    tempPath = new Path(getConf().get(DEFAULT_TEMP_KEY, DEFAULT_TEMP_PATH));

    List<String> argsList = new LinkedList<>(Arrays.asList(args));
    String inputFile = StringUtils.popOptionWithArgument("-input", argsList);
    if (inputFile == null) {
      System.err.println("-input option is required.");
      return -1;
    }
    boolean delete = StringUtils.popOption("-d", argsList);
    boolean hardDelete = StringUtils.popOption("-ddd", argsList);
    verbose = StringUtils.popOption("-v", argsList);
    String concurrencyStr = StringUtils.popOptionWithArgument("-concurrency", argsList);
    concurrency = concurrencyStr == null ? 1 : Integer.parseInt(concurrencyStr);
    processInputFile(inputFile);
    fs = FileSystem.get(getConf());

    if (hardDelete) {
      LOG.info("-ddd option specified, all paths will be deleted without sending to trash.");
      action = CleanupAction.DELETE;
    } else if (delete) {
      LOG.info("-d option specified, all paths will be sent to trash.");
      action = CleanupAction.TRASH;
    } else {
      action = CleanupAction.TEMP;
    }

    return handlePaths() ? 0 : 1;
  }

  private boolean handlePaths() throws ExecutionException, InterruptedException, IOException {
    datePath = new Path(tempPath, DLM_DATE_FORMAT.format(new Date()));
    if (action == CleanupAction.TEMP) {
      LOG.info("Using this temp path {} for current time.", datePath);
      if (!fs.exists(datePath)) {
        fs.mkdirs(datePath);
      }
    }

    int succeeded = 0;
    List<Path> failed = new ArrayList<>();
    if (concurrency == 1) {
      for (Path path : paths) {
        boolean result = handlePath(path);
        if (result) {
          succeeded++;
        } else {
          failed.add(path);
        }
      }
    } else {
      ExecutorService threadPool = Executors.newFixedThreadPool(concurrency);
      List<Callable<Pair<Path, Boolean>>> tasks = new ArrayList<>();
      for (Path path : paths) {
        tasks.add(() -> Pair.of(path, handlePath(path)));
      }
      List<Future<Pair<Path, Boolean>>> futures =
          tasks.stream().map(threadPool::submit).collect(Collectors.toList());
      for (Future future : futures) {
        Pair<Path, Boolean> result = (Pair<Path, Boolean>) future.get();
        if (result.getRight()) {
          succeeded++;
        } else {
          failed.add(result.getLeft());
        }
      }
    }
    LOG.info("All paths processed. Succeeded={}, failed={}. Failed paths are written to failed.out.",
        succeeded, failed.size());
    if (!failed.isEmpty()) {
      try (BufferedWriter writer = new BufferedWriter(
          new OutputStreamWriter(Files.newOutputStream(new File("failed.out").toPath())))) {
        for (Path path : failed) {
          writer.write(path.toString());
          writer.write("\n");
        }
      }
    }
    return failed.isEmpty();
  }

  private boolean handlePath(Path path)
      throws IOException {
    boolean result;
    if (action == CleanupAction.DELETE) {
      result = fs.delete(path);
    } else if (action == CleanupAction.TRASH) {
      result = Trash.moveToAppropriateTrash(fs, path, getConf());
    } else {
      // action == CleanupAction.TEMP
      Path destination = Path.mergePaths(datePath, path);
      try {
        fs.mkdirs(destination.getParent(),
            new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));
      } catch (FileAlreadyExistsException ignored) {
      }
      // Destination already existing might be a problem but just consider it a failure without
      // further processing.
      result = fs.rename(path, destination);
    }
    logAction(path, result);
    return result;
  }

  private void logAction(Path path, boolean result) {
    if (!verbose) {
      return;
    }
    synchronized (LOG) {
      LOG.info("action={},path={},result={}", action, path, result);
    }
  }

  private void processInputFile(String inputFile) throws IOException {
    Set<Path> result = new HashSet<>();
    try (BufferedReader br = new BufferedReader(new FileReader(inputFile))) {
      while (true) {
        String line = br.readLine();
        if (line == null) {
          break;
        }
        line = line.trim();
        if (line.isEmpty()) {
          continue;
        }
        Path path = new Path(line);
        // Verify path?
        if (path.toString().contains(" ")) {
          throw new IOException("Path " + path + " contains blank space characters");
        }
        result.add(path);
      }
    }
    paths = result;
  }

  public static void main(String[] argv) {
    Configuration conf = new HdfsConfiguration();
    DataCleanup tool = new DataCleanup(conf);
    int exitCode;
    try {
      exitCode = ToolRunner.run(tool, argv);
    } catch (Exception e) {
      LOG.error("Failed to run MigrationTool", e);
      exitCode = -1;
    }
    System.exit(exitCode);
  }
}
