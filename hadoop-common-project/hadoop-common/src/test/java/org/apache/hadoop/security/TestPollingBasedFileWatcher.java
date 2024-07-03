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

package org.apache.hadoop.security;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.junit.Test;

import static org.apache.hadoop.security.PollingBasedFileWatcher.NAMENODE_SIGNAL;

public class TestPollingBasedFileWatcher {

  @Test
  public void testPollingBasedFileWatcher() throws Exception {
    File tempFile = Files.createTempFile("testPollingBasedFileWatcher", null).toFile();
    AtomicBoolean changed = new AtomicBoolean();

    PollingBasedFileWatcher watcher = new PollingBasedFileWatcher() {
      @Override
      public boolean onModified() {
        return changed.compareAndSet(false, true);
      }
    };
    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeys.IPC_SERVER_RPC_CATEGORY_INTERNAL, NAMENODE_SIGNAL);
    watcher.setConf(conf);

    watcher.updateParams(tempFile.getAbsolutePath(), 500, 10000);

    GenericTestUtils.waitFor(() -> changed.get(), 100, 2000);
    changed.set(false);
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile))) {
      writer.write("a");
    }
    tempFile.setLastModified(Time.now() + 10000);
    GenericTestUtils.waitFor(changed::get, 100, 2000);
  }
}
