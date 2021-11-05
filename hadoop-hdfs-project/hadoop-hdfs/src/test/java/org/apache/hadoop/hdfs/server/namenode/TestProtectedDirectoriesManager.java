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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_PROTECTED_DIRECTORIES_USE_FILE_ENABLED_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_PROTECTED_DIRECTORIES_FILE_KEY;


public class TestProtectedDirectoriesManager {

  /*
   * Content
   * #/user/a
   * /user/b
   * /user/c/d
   */
  private final String protectedDirectoriesFile = "test-protecteddirectoriesfile.txt";

  @Test
  public void testProtectedDirectoriesManager() {
    Configuration conf = new Configuration();
    ProtectedDirectoriesManager.instance.reload(conf);

    // By default, fs.protected.directories.use.file.enabled is false.
    Assert.assertFalse(ProtectedDirectoriesManager.getInstance().getProtectedDirectoriesUseFileEnabled());

    String path = TestProtectedDirectoriesManager.class.getClassLoader()
        .getResource(protectedDirectoriesFile).getFile();
    conf.setBoolean(FS_PROTECTED_DIRECTORIES_USE_FILE_ENABLED_KEY, true);
    conf.set(FS_PROTECTED_DIRECTORIES_FILE_KEY, path);
    ProtectedDirectoriesManager.getInstance().reload(conf);

    // Protected directories should be contained.
    Assert.assertTrue(ProtectedDirectoriesManager.getInstance()
        .getProtectedDirectoriesUseFileEnabled());
    Assert.assertFalse(ProtectedDirectoriesManager.getInstance()
        .getProtectedDirectoriesSet().contains("/user/a"));
    Assert.assertTrue(ProtectedDirectoriesManager.getInstance()
        .getProtectedDirectoriesSet().contains("/user/b"));
    Assert.assertTrue(ProtectedDirectoriesManager.getInstance()
        .getProtectedDirectoriesSet().contains("/user/c/d"));
  }
}
