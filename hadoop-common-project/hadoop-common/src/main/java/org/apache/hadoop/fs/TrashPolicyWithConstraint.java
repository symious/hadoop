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
package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class TrashPolicyWithConstraint extends TrashPolicyDefault {
  private static final Logger LOG =
      LoggerFactory.getLogger(TrashPolicyWithConstraint.class);
  private static final String TRASH_ROOT = "/Trash";
  private static final Path CURRENT = new Path("Current");

  private TrashPolicyWithConstraint() {}

  @Override
  public void initialize(Configuration conf, FileSystem fs) {
    super.initialize(conf, fs);
  }

  @Override
  protected void moveToTrashInternal(FileSystem fs, Path srcPath, Path trashPath)
      throws IOException {
    processPathWithConstraint(fs, srcPath, trashPath, this.trashConstraint, false);
  }

  @Override
  protected boolean deleteFromTrashInternal(Path path, boolean deleteDirs) {
    try {
      FileStatus stat = fs.getFileStatus(path);
      // When deleteDirs is false, handle delete empty directory and file.
      if (!deleteDirs) {
        if (stat.isDirectory() && fileCount(path) != 0) {
          return false;
        } else {
          processPath(path, true, null);
        }
      } else {
        processPathWithConstraint(fs, path, path, this.trashConstraint, true);
      }
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  private void processPathWithConstraint(FileSystem fs, Path path, Path trashPath,
      long constraint, boolean deleteFlag) throws IOException {
    if (constraint == 0) {
      LOG.warn("The constraint is 0. Won't delete any file");
      return;
    }
    // double check to avoid mis-deletion
    if (deleteFlag && !path.toUri().getPath().startsWith(TRASH_ROOT)) {
      LOG.warn("Trying to directly delete {} which not start with {}. Please double check it.",
          path, TRASH_ROOT);
      return;
    }

    // 1. constraint > 0: File count is less than constraint will delete directly here.
    // 2. constraint < 0: Dir/File will delete directly no matter how many files it contains
    long fileCount = fileCount(path);
    if (fileCount <= constraint || constraint < 0) {
      processPath(path, deleteFlag, trashPath);
      return;
    }

    Path trashRoot = fs.getTrashRoot(path);
    Path trashCurrent = new Path(trashRoot, CURRENT);
    FileStatus[] stats = fs.listStatus(path);
    // Process the path
    for (FileStatus stat : stats) {
      if (stat.isFile()) {
        Path fileTrashPath = makeTrashRelativePath(trashCurrent, stat.getPath());
        if (createTrashRootEnable && !fs.exists(fileTrashPath.getParent())) {
          fs.mkdirs(fileTrashPath.getParent(), new FsPermission(PERMISSION));
        }
      } else {
        Path subTrashPath = makeTrashRelativePath(trashCurrent, stat.getPath());
        if (createTrashRootEnable && !fs.exists(subTrashPath.getParent())) {
          fs.mkdirs(subTrashPath.getParent(), new FsPermission(PERMISSION));
        }
        processPathWithConstraint(fs, stat.getPath(), subTrashPath, constraint,
            deleteFlag);
      }
    }
    // After rename all the file in the directory, the empty directory can be deleted.
    if (fileCount(path) == 0) {
      processPath(path, true, trashPath);
    } else {
      LOG.warn("Directory " + path + " is not be deleted successfully");
    }
  }

  /**
   * Count number of files
   */
  private long fileCount(Path path) throws IOException {
    ContentSummary summary = fs.getContentSummary(path);
    return summary.getFileCount();
  }

  /**
   * Delete or rename path according to deleteFlag
   */
  private void processPath(Path path, boolean deleteFlag, Path trashPath) throws IOException {
    if (deleteFlag) {
      if (!fs.delete(path, true)) {
        throw new IOException("Delete " + path + " fail!");
      }
    } else {
      LOG.debug("Moving {} to {}.", path, trashPath);
      fs.rename(path, trashPath, Options.Rename.TO_TRASH);
    }
  }
}