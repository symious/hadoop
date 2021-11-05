/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RefreshHandler;
import org.apache.hadoop.ipc.RefreshRegistry;
import org.apache.hadoop.ipc.RefreshResponse;

import java.io.File;
import java.io.IOException;
import java.util.SortedSet;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_PROTECTED_DIRECTORIES_USE_FILE_ENABLED_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_PROTECTED_DIRECTORIES_USE_FILE_ENABLED_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_PROTECTED_DIRECTORIES_FILE_KEY;

/**
 * Refresh protected directories
 */
public class ProtectedDirectoriesManager extends GenericBasedOnFileManager
    implements RefreshHandler {

  private static final Log LOG = LogFactory.getLog(ProtectedDirectoriesManager.class);

  private static final String REFRESH_PROTECTED_DIRECTORIES = "REFRESH_PROTECTED_DIRECTORIES";

  private volatile boolean protectedDirectoriesUseFileEnabled = false;

  protected static final ProtectedDirectoriesManager instance = new ProtectedDirectoriesManager();

  static {
    RefreshRegistry.defaultRegistry().register(REFRESH_PROTECTED_DIRECTORIES, instance);
  }

  private ProtectedDirectoriesManager() {
    Configuration conf = new Configuration();
    // Load protected directories.
    reload(conf);
  }

  /**
   * singleton pattern.
   */
  public static ProtectedDirectoriesManager getInstance() {
    return instance;
  }

  @Override
  public RefreshResponse handleRefresh(String identifier, String[] args) {

    if (identifier.equals(REFRESH_PROTECTED_DIRECTORIES)) {

      Configuration conf = new Configuration();
      reload(conf);

      return RefreshResponse.successResponse();
    }

    return new RefreshResponse(-1, "Invalid identifier: " + identifier);
  }

  /**
   * Reload protected directories.
   *
   * @param conf not null
   * @throws IOException load failed
   */
  public void reload(Configuration conf) {
    this.protectedDirectoriesUseFileEnabled = conf.getBoolean(
        FS_PROTECTED_DIRECTORIES_USE_FILE_ENABLED_KEY,
        FS_PROTECTED_DIRECTORIES_USE_FILE_ENABLED_DEFAULT);
    LOG.info("Protected directories use file enabled: "
        + protectedDirectoriesUseFileEnabled);

    if (!protectedDirectoriesUseFileEnabled) {
      return;
    }

    try {
      loadProtectedDirectories(conf);
    } catch (IOException e) {
      LOG.error("Error reloading protected directories. ", e);
    }
  }

  private void loadProtectedDirectories(Configuration conf) throws IOException {
    String protectedDirectoriesFile = conf.get(
        FS_PROTECTED_DIRECTORIES_FILE_KEY);
    if (protectedDirectoriesFile == null
        || protectedDirectoriesFile.isEmpty()) {
      LOG.error(FS_PROTECTED_DIRECTORIES_FILE_KEY
          + " not configured.");
      return;
    }

    File file = new File(protectedDirectoriesFile);
    loadElements(file);
  }

  public boolean getProtectedDirectoriesUseFileEnabled() {
    return this.protectedDirectoriesUseFileEnabled;
  }

  public SortedSet<String> getProtectedDirectoriesSet() {
    return getElementsSet();
  }
}