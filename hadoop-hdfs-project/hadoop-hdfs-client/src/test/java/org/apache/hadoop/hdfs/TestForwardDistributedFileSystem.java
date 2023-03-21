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

package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TestForwardDistributedFileSystem {

  private static class CloseMatterFs extends LocalFileSystem {
    private boolean mClosed = false;

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
      if (mClosed) {
        throw new IOException("CloseMatterFs already closed! ");
      }
      return super.getFileStatus(new Path("file://" + path.getName()));
    }

    @Override
    public void close() throws IOException {
      if (!mClosed) {
        mClosed = true;
        super.close();
      }
    }
  }

  @Test
  public void testConcurrency() throws Exception {
    int n = 3;
    ExecutorService executorService = Executors.newFixedThreadPool(n);
    ArrayList<Future<?>> futures = new ArrayList<>();
    for (int i=0; i<n; i++) {
      futures.add(executorService.submit(() -> {
        final Configuration conf = new Configuration();
        conf.set("fs.forward.impl", ForwardDistributedFileSystem.class.getName());
        conf.set(CommonConfigurationKeysPublic.FS_FORWARD_RULES, "forward://host,close://host");
        conf.set("fs.close.impl", CloseMatterFs.class.getName());
        final URI proxyUri = URI.create("forward://host/");
        try {
          FileSystem fs = FileSystem.get(proxyUri, conf);
          Thread.sleep(10);
          fs.getFileStatus(new Path("forward://host/"));
        } catch (IOException e) {
          Assert.fail(e.toString());
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }));
    }
    for (int i=0; i<n; i++) {
      futures.get(i).get();
    }
  }
}
