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
package org.apache.hadoop.hdfs.tools;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.hdfs.BlockMissingException;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A tool that takes in a file with a list of paths that supposedly
 * have only 1 replica, verifies if they really have only 1 replica,
 * handles those paths accordingly:
 * <li>if all blocks are present, increase the file replica to 2,</li>
 * <li>if some blocks are missing, the file is unsalvageable at this point, delete it.</li>
 * Will output a preview file by default. Real setReplication/moveToTrash operations are done
 * with an extra command argument.
 */
public class VerifySingleReplica extends Configured implements Tool {
  private final static Logger LOG = LoggerFactory.getLogger(VerifySingleReplica.class);

  @Override
  public int run(String[] args) throws Exception {
    String description = "Usage: hdfs singleReplica -p PREVIEW_FILE [-i INPUT_FILE] [--execute]\n"
        + "\tVerifies if some paths have only 1 replica, handles those paths accordingly.\n"
        + "\tOutputs a preview files without executing any mutation operations by default.\n"
        + "\tRequired command line arguments:\n"
        + "\t-p,--previewFile <arg> Path to output preview file "
        +                          "showing what paths to delete/update to 2 replicas.\n"
        + "\n"
        + "\tOptional command line arguments:\n"
        + "\t-i,--inputFile   <arg> Path to file containing a list of HDFS paths to check. "
        +                          "Ignored if --execute is used.\n"
        + "\t-e,--execute           Read the preview output file and carry out the operations.\n";

    if (args.length == 0) {
      System.out.println(description);
      return 0;
    }

    Options options = buildOptions();

    CommandLineParser parser = new PosixParser();
    CommandLine cmd;

    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      System.out.println("Error parsing command-line options: ");
      System.out.println(description);
      return -1;
    }

    String inputFile = cmd.getOptionValue("i");
    String previewFile = cmd.getOptionValue("p");
    boolean execute = cmd.hasOption("e");

    if (execute && inputFile != null) {
      System.out.println("inputFile will be ignored in execute mode.");
    }

    // Just let it throw exception if casting fails.
    DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.get(getConf());

    if (!execute) {
      verifyPathsAndSavePreview(dfs, inputFile, previewFile);
    } else {
      deleteOrSetReplicationForPaths(dfs, previewFile);
    }

    return 0;
  }

  private void verifyPathsAndSavePreview(DistributedFileSystem dfs, String inputFile, String previewFile)
      throws IOException {
    // Map of file paths -> file having a missing block or not.
    Map<Path, Boolean> result = new HashMap<>();

    int counter = 0;
    try (BufferedReader br = new BufferedReader(new FileReader(inputFile))) {
      while (true) {
        String line = br.readLine();
        counter++;
        if (line == null) {
          break;
        }
        if (counter % 100 == 0) {
          LOG.info("Processed {} lines", counter);
        }
        line = line.trim();
        if (line.isEmpty()) {
           continue;
        }
        Path path = new Path(line);
        RemoteIterator<LocatedFileStatus> ite = dfs.listFiles(path, true);
        while (ite.hasNext()) {
          verifyFile(dfs, ite.next(), result);
        }
      }
    }

    try (BufferedWriter bw = new BufferedWriter(new FileWriter(previewFile))) {
      for (Map.Entry<Path, Boolean> entry : result.entrySet()) {
        Path path = entry.getKey();
        boolean hasMissingBlocks = entry.getValue();
        bw.write(path.toUri().getPath());
        bw.write(",");
        bw.write(hasMissingBlocks ? "1" : "0");
        bw.write("\n");
      }
      bw.flush();
    }
  }

  /**
   * Check if the file has 1 replica or not, and if it does, does it have any missing blocks?
   * Return immediately if file has more than 1 replicas.
   * @param fileStatus {@link LocatedFileStatus} of the file to check
   * @param result map to put verification result into
   */
  private void verifyFile(DistributedFileSystem dfs, LocatedFileStatus fileStatus, Map<Path, Boolean> result)
      throws IOException {
    if (fileStatus.getReplication() != 1) {
      return ;
    }

    // Try basic scan first
    fileStatus = dfs.listLocatedStatus(fileStatus.getPath()).next();
    for (BlockLocation block: fileStatus.getBlockLocations()) {
      if (block.getHosts().length == 0) {
        result.put(fileStatus.getPath(), true);
        return;
      }
    }
    byte[] buff = new byte[4];

    try {
      for (BlockLocation block: fileStatus.getBlockLocations()) {
        dfs.open(fileStatus.getPath()).read(block.getOffset(), buff, 0, 1);
      }
    } catch (BlockMissingException bme) {
      result.put(fileStatus.getPath(), true);
      return;
    } catch (IOException e) {
      LOG.warn("Unexpected exception while trying to read {}", fileStatus.getPath(), e);
    }
    result.put(fileStatus.getPath(), false);
  }

  private void deleteOrSetReplicationForPaths(DistributedFileSystem dfs, String previewFile)
      throws IOException {
    if (!Files.exists(Paths.get(previewFile))) {
      throw new FileNotFoundException("Need to generate a preview file first!");
    }
    Set<Path> pathsToDelete = new HashSet<>();
    Set<Path> pathsToSetReplica = new HashSet<>();

    try (BufferedReader br = new BufferedReader(new FileReader(previewFile))) {
      String line = br.readLine();
      while (line != null) {
        String[] lineSplit = line.trim().split(",");
        Path path = new Path(lineSplit[0]);
        // Missing blocks
        if (Objects.equals(lineSplit[1], "1")) {
          pathsToDelete.add(path);
        } else {
          pathsToSetReplica.add(path);
        }
        line = br.readLine();
      }
    }

    for (Path path : pathsToSetReplica) {
      dfs.setReplication(path, (short) 2);
    }
    Trash trash = new Trash(dfs, dfs.getConf());
    for (Path path : pathsToDelete) {
      trash.moveToTrash(path);
    }
  }

  private static Options buildOptions() {
    Options options = new Options();

    OptionBuilder.isRequired();
    OptionBuilder.hasArgs();
    OptionBuilder.withLongOpt("previewFile");
    options.addOption(OptionBuilder.create("p"));

    options.addOption("i", "inputFile", true, "");
    options.addOption("e", "execute", false, "");

    return options;
  }

  public static void main(String[] argv) throws Exception {
    int rc = ToolRunner.run(new VerifySingleReplica(), argv);
    System.exit(rc);
  }
}
