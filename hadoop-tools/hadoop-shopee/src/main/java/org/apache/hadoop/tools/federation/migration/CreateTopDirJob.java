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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.util.StringUtils;

/**
 * One of the supported commands for {@link NSMigrationTool}.
 * Creates dirs with the same attributes, permissions, ownership in a destination namespace,
 * using values from existing dirs in a source namespace.
 */
public class CreateTopDirJob {
  private final DistributedFileSystem dstFs;
  private final DistributedFileSystem srcFs;
  private final Set<Path> paths;

  public CreateTopDirJob(String path, String inputFile, String srcNs, String dstNs,
      Configuration conf) throws IOException {
    this.dstFs = (DistributedFileSystem) FileSystem.get(URI.create("hdfs://" + dstNs), conf);
    this.srcFs = (DistributedFileSystem) FileSystem.get(URI.create("hdfs://" + srcNs), conf);
    this.paths = MigrationUtils.loadPaths(new Path(path), inputFile);
  }

  public static int handleArgs(List<String> argsList, Configuration conf) throws IOException {
    String path = StringUtils.popOptionWithArgument("-path", argsList);
    String input = StringUtils.popOptionWithArgument("-input", argsList);
    if (path == null && input == null) {
      System.err.println("Either -path or -input option is required.");
      return -1;
    }
    String src = StringUtils.popOptionWithArgument("-src", argsList);
    String dst = StringUtils.popOptionWithArgument("-dst", argsList);
    if (src == null || dst == null) {
      System.err.println("-src and -dst options are required.");
      return -1;
    }
    CreateTopDirJob job = new CreateTopDirJob(path, input, src, dst, conf);
    return job.execute();
  }

  public int execute() throws IOException {
    for (Path path : paths) {
      if (!createDirsWithPermission(srcFs, dstFs, path, true)) {
        return 1;
      }
    }
    return 0;
  }

  public static boolean createDirsWithPermission(DistributedFileSystem srcFs,
      DistributedFileSystem dstFs, Path inputPath, boolean firstRun) throws IOException {
    List<Path> allPathsFromRoot = new ArrayList<>();
    Path cur = inputPath;
    while (!cur.isRoot()) {
      allPathsFromRoot.add(cur);
      cur = cur.getParent();
    }
    Collections.reverse(allPathsFromRoot);
    // If called from PREPARE stage in MigrationJob, can skip the first 2 levels (/projects/project)
    if (!firstRun) {
      allPathsFromRoot = allPathsFromRoot.subList(2, allPathsFromRoot.size());
    }

    boolean allSuccessful = true;
    for (Path path : allPathsFromRoot) {
      if (dstFs.exists(path)) {
        continue;
      }
      FileStatus fileStatus = srcFs.getFileStatus(inputPath);
      allSuccessful &= dstFs.mkdir(path, fileStatus.getPermission());
      dstFs.setOwner(path, fileStatus.getOwner(), fileStatus.getGroup());
      dstFs.setAcl(path, srcFs.getAclStatus(path).getEntries());
      Map<String, byte[]> srcXAttrs = srcFs.getXAttrs(path);
      for (Map.Entry<String, byte[]> entry : srcXAttrs.entrySet()) {
        String xattrName = entry.getKey();
        dstFs.setXAttr(path, xattrName, entry.getValue());
      }
    }
    return allSuccessful;
  }
}
