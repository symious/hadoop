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
package org.apache.hadoop.hdfs.server.zoneservice;

import java.io.IOException;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.balancer.ExitStatus;
import org.apache.hadoop.hdfs.server.zoneservice.metrics.ZoneProgressTracker;
import org.apache.hadoop.net.StaticMapping;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.FakeTimer;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdfs.server.zoneservice.metrics.ZoneProgressTracker.UNTRACKED_DUMMY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestZoneProgressTracker {
  private MiniDFSCluster cluster = null;

  @BeforeClass
  public static void setLogging() {
    LogManager.getLogger(ZoneProgressTracker.class.getName()).setLevel(Level.DEBUG);
  }

  @Before
  public void reset() {
    ZoneProgressTracker.resetTracker();
  }

  @After
  public void teardown() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  public void testTrackerPrinter() {
    Configuration conf = new Configuration();
    GenericTestUtils.LogCapturer capture = GenericTestUtils.LogCapturer.captureLogs(LoggerFactory.getLogger(ZoneProgressTracker.class));
    // Test print every 5 files
    int FILES_PER_PRINT = 5;
    ZoneProgressTracker.resetTracker();
    conf.setInt(DFSConfigKeys.DFS_ZONE_PROGRESS_TRACKER_FILES_PER_PRINT_KEY, FILES_PER_PRINT);
    conf.setEnum(DFSConfigKeys.DFS_ZONE_PROGRESS_TRACKER_PRINT_MODE_KEY, ZoneProgressTracker.ZoneProgressPrintModes.EVERY_N_FILES);
    ZoneProgressTracker.initConf(conf);
    for (int i = 0; i < 3; i++) {
      capture.clearOutput();
      for (int j = 0; j < FILES_PER_PRINT - 1; j++) {
        bumpDummyFileProcessed();
        assertFalse(capture.getOutput().contains("Zoneservice progress"));
      }
      bumpDummyFileProcessed();
      assertTrue(capture.getOutput().contains("Zoneservice progress"));
    }

    // Tests print every 3 seconds
    ZoneProgressTracker.resetTracker();
    conf.setLong(DFSConfigKeys.DFS_ZONE_PROGRESS_TRACKER_PRINT_PERIOD_KEY, 3000);
    conf.setEnum(DFSConfigKeys.DFS_ZONE_PROGRESS_TRACKER_PRINT_MODE_KEY, ZoneProgressTracker.ZoneProgressPrintModes.PERIODICALLY);
    ZoneProgressTracker.initConf(conf);
    FakeTimer timer = new FakeTimer();
    timer.advance(1000000);
    ZoneProgressTracker.setTimer(timer);

    // The first print always passes through
    bumpDummyFileProcessed();
    timer.advance(1000);
    assertTrue(capture.getOutput().contains("Zoneservice progress"));

    capture.clearOutput();
    bumpDummyFileProcessed();
    assertFalse(capture.getOutput().contains("Zoneservice progress"));
    timer.advance(2500);
    bumpDummyFileProcessed();
    assertTrue(capture.getOutput().contains("Zoneservice progress"));
  }

  @Test
  public void testTrackPaths() throws IOException, InterruptedException {
    // No error thrown for non-existent paths
    Configuration conf = TestUtils.getConf();
    cluster = new MiniDFSCluster.Builder(conf).build();
    cluster.waitActive();
    DistributedFileSystem fs = cluster.getFileSystem();

    try {
      ZoneProgressTracker.trackPaths(fs, Collections.singletonList(new Path("/pathdoesnotexist")));
    } catch (Exception e) {
      fail("No exceptions should be thrown for non-existent paths.");
    }
    ZoneProgressTracker.waitForTotalFilesTrackerToFinish();

    // Disable ETC
    GenericTestUtils.LogCapturer capture = GenericTestUtils.LogCapturer.captureLogs(
        LoggerFactory.getLogger(ZoneProgressTracker.class));
    ZoneProgressTracker.resetTracker();
    conf.setBoolean(DFSConfigKeys.DFS_ZONE_PROGRESS_TRACKER_ESTIMATE_COMPLETION_TIME_KEY, false);
    ZoneProgressTracker.initConf(conf);
    fs.mkdir(new Path("/testTrackPaths"), new FsPermission("777"));
    Path testPath = new Path("/testTrackPaths/dummy");
    DFSTestUtil.createFile(fs, testPath, 1, (short) 3, 0L);
    ZoneProgressTracker.trackPaths(fs, Collections.singletonList(testPath));
    ZoneProgressTracker.waitForTotalFilesTrackerToFinish();
    ZoneProgressTracker.incrFileCount();

    assertEquals(ZoneProgressTracker.getTrackedTotalFiles(), UNTRACKED_DUMMY);
  }

  @Test
  public void testZoneMoverTracker() throws Exception {
    int FILE_COUNT = 10;
    int FILE_LEN = 1 << 20;
    short REPLICAS = 3;
    long ESTIMATED_THROTTLED_RUNTIME = 15;
    long totalBytes = REPLICAS * FILE_COUNT * (FILE_COUNT + 1) / 2 * FILE_LEN;
    String TEST_PATH = "/testZoneMoverTracker";

    StaticMapping.resetMap();
    final String[] hosts1 = { "host0", "host1", "host2" };
    final String[] racks1 = { "/dc0/rack0", "/dc0/rack0", "/dc0/rack1" };
    Configuration conf = TestUtils.getConf();
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, REPLICAS);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, FILE_LEN * FILE_COUNT / 4);
    // Limit bandwidth so the process take around 30 seconds
    conf.setLong(DFSConfigKeys.DFS_ZONEMOVER_DISPATCHER_THROTTLER_BANDWIDTH_KEY,
        totalBytes / ESTIMATED_THROTTLED_RUNTIME);
    cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(hosts1.length).hosts(hosts1).racks(racks1)
            .build();
    cluster.waitActive();
    DistributedFileSystem fs = cluster.getFileSystem();
    fs.mkdir(new Path(TEST_PATH), new FsPermission("777"));

    // Create a bunch of files that span multiple blocks
    int expectedBlocks = 0;
    for (int i = 0; i < FILE_COUNT; i++) {
      Path path = new Path(TEST_PATH + "/testTracker" + i + ".txt");
      DFSTestUtil.createFile(fs, path, (i + 1) * FILE_LEN, REPLICAS, 0L);
      expectedBlocks += Math.ceil((double) (i + 1) * 4 / FILE_COUNT) * REPLICAS;
    }

    final String[] hosts2 = { "host3", "host4", "host5" };
    final String[] racks2 = { "/dc1/rack0", "/dc1/rack1", "/dc1/rack2" };
    cluster.startDataNodes(conf, hosts2.length, true, null, racks2, hosts2, null, false);

    long start = Time.monotonicNow();
    Tool tool = new ZoneMover.Cli();
    tool.setConf(conf);
    final String[] args = { "-path", TEST_PATH, "-rule", "/dc1:3" };
    assertEquals(ExitStatus.SUCCESS.getExitCode(), tool.run(args));
    double runtime = (double) (Time.monotonicNow() - start) / 1000;

    assertTrue(Math.abs(runtime / ESTIMATED_THROTTLED_RUNTIME - 1) < 0.3);
    assertEquals(FILE_COUNT, ZoneProgressTracker.getFileCount());
    assertEquals(expectedBlocks, ZoneProgressTracker.getBlockCount());
    assertEquals(totalBytes, ZoneProgressTracker.getByteCount());
  }

  private void bumpDummyFileProcessed() {
    ZoneProgressTracker.queueFile("/testzzz");
    ZoneProgressTracker.incrBlockCount();
    ZoneProgressTracker.addByteCount(1234);
    ZoneProgressTracker.dequeueFile("/testzzz");
  }
}
