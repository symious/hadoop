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
package org.apache.hadoop.hdfs.server.federation.resolver;

import static org.apache.hadoop.hdfs.server.federation.resolver.order.HashResolver.extractTempFileName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.resolver.order.DestinationOrder;
import org.apache.hadoop.hdfs.server.federation.store.records.MountTable;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the multiple destination resolver.
 */
public class TestMultipleDestinationResolver {

  private MultipleDestinationMountTableResolver resolver;

  @Before
  public void setup() throws IOException {
    Configuration conf = new Configuration();
    resolver = new MultipleDestinationMountTableResolver(conf, null);

    // We manually point /tmp to only subcluster0
    Map<String, String> map1 = new HashMap<>();
    map1.put("subcluster0", "/tmp");
    resolver.addEntry(MountTable.newInstance("/tmp", map1));

    // We manually point / to subcluster0,1,2 with default order (hash)
    Map<String, String> mapDefault = new HashMap<>();
    mapDefault.put("subcluster0", "/");
    mapDefault.put("subcluster1", "/");
    mapDefault.put("subcluster2", "/");
    MountTable defaultEntry = MountTable.newInstance("/", mapDefault);
    resolver.addEntry(defaultEntry);

    // We manually point /hash to subcluster0,1,2 with hashing
    Map<String, String> mapHash = new HashMap<>();
    mapHash.put("subcluster0", "/hash");
    mapHash.put("subcluster1", "/hash");
    mapHash.put("subcluster2", "/hash");
    MountTable hashEntry = MountTable.newInstance("/hash", mapHash);
    hashEntry.setDestOrder(DestinationOrder.HASH);
    resolver.addEntry(hashEntry);

    // We manually point /hashall to subcluster0,1,2 with hashing (full tree)
    Map<String, String> mapHashAll = new HashMap<>();
    mapHashAll.put("subcluster0", "/hashall");
    mapHashAll.put("subcluster1", "/hashall");
    mapHashAll.put("subcluster2", "/hashall");
    MountTable hashEntryAll = MountTable.newInstance("/hashall", mapHashAll);
    hashEntryAll.setDestOrder(DestinationOrder.HASH_ALL);
    resolver.addEntry(hashEntryAll);

    // We point /local to subclusters 0, 1, 2 with the local order
    Map<String, String> mapLocal = new HashMap<>();
    mapLocal.put("subcluster0", "/local");
    mapLocal.put("subcluster1", "/local");
    mapLocal.put("subcluster2", "/local");
    MountTable localEntry = MountTable.newInstance("/local", mapLocal);
    localEntry.setDestOrder(DestinationOrder.LOCAL);
    resolver.addEntry(localEntry);

    // We point /random to subclusters 0, 1, 2 with the random order
    Map<String, String> mapRandom = new HashMap<>();
    mapRandom.put("subcluster0", "/random");
    mapRandom.put("subcluster1", "/random");
    mapRandom.put("subcluster2", "/random");
    MountTable randomEntry = MountTable.newInstance("/random", mapRandom);
    randomEntry.setDestOrder(DestinationOrder.RANDOM);
    resolver.addEntry(randomEntry);

    // Read only mount point
    Map<String, String> mapReadOnly = new HashMap<>();
    mapReadOnly.put("subcluster0", "/readonly");
    mapReadOnly.put("subcluster1", "/readonly");
    mapReadOnly.put("subcluster2", "/readonly");
    MountTable readOnlyEntry = MountTable.newInstance("/readonly", mapReadOnly);
    readOnlyEntry.setReadOnly(true);
    resolver.addEntry(readOnlyEntry);

    // We point /suffix to subclusters 0, 1, 2 with the suffix order
    Map<String, String> mapSuffix = new HashMap<>();
    mapSuffix.put("subcluster0", "/suffix");
    mapSuffix.put("subcluster1", "/suffix");
    mapSuffix.put("subcluster2", "/suffix");
    mapSuffix.put("subcluster6", "/suffix6");
    mapSuffix.put("subcluster7", "/suffix7");
    MountTable suffixEntry = MountTable.newInstance("/suffix", mapSuffix);
    suffixEntry.setDestOrder(DestinationOrder.SUFFIX);
    resolver.addEntry(suffixEntry);
    // point /dir0 to only subcluster0
    Map<String, String> tmpMap = new HashMap<>();
    tmpMap.put("subcluster0", "/dir0");
    resolver.addEntry(MountTable.newInstance("/dir0", tmpMap));
    // point /dir1 to only subcluster1
    tmpMap = new HashMap<>();
    tmpMap.put("subcluster1", "/dir1");
    resolver.addEntry(MountTable.newInstance("/dir1", tmpMap));
    // point /dir2 to only subcluster2
    tmpMap = new HashMap<>();
    tmpMap.put("subcluster2", "/dir2");
    resolver.addEntry(MountTable.newInstance("/dir2", tmpMap));
    // point /dir3 to only subcluster3
    tmpMap = new HashMap<>();
    tmpMap.put("subcluster3", "/dir3");
    resolver.addEntry(MountTable.newInstance("/dir3", tmpMap));
    // point /dir5 to subclusters 0, 1, 2 with the suffix order
    mapSuffix = new HashMap<>();
    mapSuffix.put("subcluster0", "/dir5");
    mapSuffix.put("subcluster1", "/dir5");
    mapSuffix.put("subcluster2", "/dir5");
    suffixEntry = MountTable.newInstance("/dir5", mapSuffix);
    suffixEntry.setDestOrder(DestinationOrder.SUFFIX);
    resolver.addEntry(suffixEntry);
    // point /dir6 to only subcluster6
    tmpMap = new HashMap<>();
    tmpMap.put("subcluster6", "/dir6");
    resolver.addEntry(MountTable.newInstance("/dir6", tmpMap));
    // point /dir7 to only subcluster7
    tmpMap = new HashMap<>();
    tmpMap.put("subcluster7", "/dir7_sub");
    resolver.addEntry(MountTable.newInstance("/dir7", tmpMap));

    Map<String, String> mapFixed = new LinkedHashMap<>();
    mapFixed.put("subcluster2", "/fixed");
    mapFixed.put("subcluster1", "/fixed");
    mapFixed.put("subcluster0", "/fixed");
    MountTable fixedEntry = MountTable.newInstance("/fixed", mapFixed);
    fixedEntry.setDestOrder(DestinationOrder.FIXED);
    resolver.addEntry(fixedEntry);
  }

  @Test
  public void testHashEqualDistribution() throws IOException {
    // First level
    testEvenDistribution("/hash");
    testEvenDistribution("/hash/folder0", false);

    // All levels
    testEvenDistribution("/hashall");
    testEvenDistribution("/hashall/folder0");
  }

  @Test
  public void testHashAll() throws IOException {
    // Files should be spread across subclusters
    PathLocation dest0 = resolver.getDestinationForPath("/hashall/file0.txt");
    assertDest("subcluster0", dest0);
    PathLocation dest1 = resolver.getDestinationForPath("/hashall/file1.txt");
    assertDest("subcluster1", dest1);

    // Files within folder should be spread across subclusters
    PathLocation dest2 = resolver.getDestinationForPath("/hashall/folder0");
    assertDest("subcluster2", dest2);
    PathLocation dest3 = resolver.getDestinationForPath(
        "/hashall/folder0/file0.txt");
    assertDest("subcluster1", dest3);
    PathLocation dest4 = resolver.getDestinationForPath(
        "/hashall/folder0/file1.txt");
    assertDest("subcluster0", dest4);

    PathLocation dest5 = resolver.getDestinationForPath(
        "/hashall/folder0/folder0/file0.txt");
    assertDest("subcluster1", dest5);
    PathLocation dest6 = resolver.getDestinationForPath(
        "/hashall/folder0/folder0/file1.txt");
    assertDest("subcluster1", dest6);
    PathLocation dest7 = resolver.getDestinationForPath(
        "/hashall/folder0/folder0/file2.txt");
    assertDest("subcluster0", dest7);

    PathLocation dest8 = resolver.getDestinationForPath("/hashall/folder1");
    assertDest("subcluster1", dest8);
    PathLocation dest9 = resolver.getDestinationForPath(
        "/hashall/folder1/file0.txt");
    assertDest("subcluster0", dest9);
    PathLocation dest10 = resolver.getDestinationForPath(
        "/hashall/folder1/file1.txt");
    assertDest("subcluster1", dest10);

    PathLocation dest11 = resolver.getDestinationForPath("/hashall/folder2");
    assertDest("subcluster2", dest11);
    PathLocation dest12 = resolver.getDestinationForPath(
        "/hashall/folder2/file0.txt");
    assertDest("subcluster0", dest12);
    PathLocation dest13 = resolver.getDestinationForPath(
        "/hashall/folder2/file1.txt");
    assertDest("subcluster0", dest13);
    PathLocation dest14 = resolver.getDestinationForPath(
        "/hashall/folder2/file2.txt");
    assertDest("subcluster1", dest14);
  }

  @Test
  public void testHashFirst() throws IOException {
    PathLocation dest0 = resolver.getDestinationForPath("/hashall/file0.txt");
    assertDest("subcluster0", dest0);
    PathLocation dest1 = resolver.getDestinationForPath("/hashall/file1.txt");
    assertDest("subcluster1", dest1);

    // All these must be in the same location: subcluster0
    PathLocation dest2 = resolver.getDestinationForPath("/hash/folder0");
    assertDest("subcluster0", dest2);
    PathLocation dest3 = resolver.getDestinationForPath(
        "/hash/folder0/file0.txt");
    assertDest("subcluster0", dest3);
    PathLocation dest4 = resolver.getDestinationForPath(
        "/hash/folder0/file1.txt");
    assertDest("subcluster0", dest4);

    PathLocation dest5 = resolver.getDestinationForPath(
        "/hash/folder0/folder0/file0.txt");
    assertDest("subcluster0", dest5);
    PathLocation dest6 = resolver.getDestinationForPath(
        "/hash/folder0/folder0/file1.txt");
    assertDest("subcluster0", dest6);

    // All these must be in the same location: subcluster2
    PathLocation dest7 = resolver.getDestinationForPath("/hash/folder1");
    assertDest("subcluster2", dest7);
    PathLocation dest8 = resolver.getDestinationForPath(
        "/hash/folder1/file0.txt");
    assertDest("subcluster2", dest8);
    PathLocation dest9 = resolver.getDestinationForPath(
        "/hash/folder1/file1.txt");
    assertDest("subcluster2", dest9);

    // All these must be in the same location: subcluster2
    PathLocation dest10 = resolver.getDestinationForPath("/hash/folder2");
    assertDest("subcluster2", dest10);
    PathLocation dest11 = resolver.getDestinationForPath(
        "/hash/folder2/file0.txt");
    assertDest("subcluster2", dest11);
    PathLocation dest12 = resolver.getDestinationForPath(
        "/hash/folder2/file1.txt");
    assertDest("subcluster2", dest12);
  }

  @Test
  public void testRandomEqualDistribution() throws IOException {
    testEvenDistribution("/random");
  }

  @Test
  public void testSingleDestination() throws IOException {
    // All the files in /tmp should be in subcluster0
    for (int f = 0; f < 100; f++) {
      String filename = "/tmp/b/c/file" + f + ".txt";
      PathLocation destination = resolver.getDestinationForPath(filename);
      RemoteLocation loc = destination.getDefaultLocation();
      assertEquals("subcluster0", loc.getNameserviceId());
      assertEquals(filename, loc.getDest());
    }
  }

  @Test
  public void testResolveSubdirectories() throws Exception {
    // Simulate a testdir under a multi-destination mount.
    Random r = new Random();
    String testDir = "/sort/testdir" + r.nextInt();
    String file1 = testDir + "/file1" + r.nextInt();
    String file2 = testDir + "/file2" + r.nextInt();

    // Verify both files resolve to the same namespace as the parent dir.
    PathLocation testDirLocation = resolver.getDestinationForPath(testDir);
    RemoteLocation defaultLoc = testDirLocation.getDefaultLocation();
    String testDirNamespace = defaultLoc.getNameserviceId();

    PathLocation file1Location = resolver.getDestinationForPath(file1);
    RemoteLocation defaultLoc1 = file1Location.getDefaultLocation();
    assertEquals(testDirNamespace, defaultLoc1.getNameserviceId());

    PathLocation file2Location = resolver.getDestinationForPath(file2);
    RemoteLocation defaultLoc2 = file2Location.getDefaultLocation();
    assertEquals(testDirNamespace, defaultLoc2.getNameserviceId());
  }

  @Test
  public void testExtractTempFileName() {
    for (String teststring : new String[] {
        "testfile1.txt.COPYING",
        "testfile1.txt._COPYING_",
        "testfile1.txt._COPYING_.attempt_1486662804109_0055_m_000042_0",
        "testfile1.txt.tmp",
        "_temp/testfile1.txt",
        "_temporary/testfile1.txt.af77e2ab-4bc5-4959-ae08-299c880ee6b8",
        "_temporary/0/_temporary/attempt_201706281636_0007_m_000003_46/" +
          "testfile1.txt" }) {
      String finalName = extractTempFileName(teststring);
      assertEquals("testfile1.txt", finalName);
    }

    // False cases
    assertEquals(
        "file1.txt.COPYING1", extractTempFileName("file1.txt.COPYING1"));
    assertEquals("file1.txt.tmp2", extractTempFileName("file1.txt.tmp2"));

    // Speculation patterns
    String finalName = extractTempFileName(
        "_temporary/part-00007.af77e2ab-4bc5-4959-ae08-299c880ee6b8");
    assertEquals("part-00007", finalName);
    finalName = extractTempFileName(
        "_temporary/0/_temporary/attempt_201706281636_0007_m_000003_46/" +
          "part-00003");
    assertEquals("part-00003", finalName);

    // Subfolders
    finalName = extractTempFileName("folder0/testfile1.txt._COPYING_");
    assertEquals("folder0/testfile1.txt", finalName);
    finalName = extractTempFileName(
        "folder0/folder1/testfile1.txt._COPYING_");
    assertEquals("folder0/folder1/testfile1.txt", finalName);
    finalName = extractTempFileName(
        "processedHrsData.txt/_temporary/0/_temporary/" +
        "attempt_201706281636_0007_m_000003_46/part-00003");
    assertEquals("processedHrsData.txt/part-00003", finalName);
  }

  @Test
  public void testReadOnly() throws IOException {
    MountTable mount = resolver.getMountPoint("/readonly");
    assertTrue(mount.isReadOnly());

    PathLocation dest0 = resolver.getDestinationForPath("/readonly/file0.txt");
    assertDest("subcluster1", dest0);
    PathLocation dest1 = resolver.getDestinationForPath("/readonly/file1.txt");
    assertDest("subcluster2", dest1);

    // All these must be in the same location: subcluster0
    PathLocation dest2 = resolver.getDestinationForPath("/readonly/folder0");
    assertDest("subcluster1", dest2);
    PathLocation dest3 = resolver.getDestinationForPath(
        "/readonly/folder0/file0.txt");
    assertDest("subcluster1", dest3);
    PathLocation dest4 = resolver.getDestinationForPath(
        "/readonly/folder0/file1.txt");
    assertDest("subcluster1", dest4);

    PathLocation dest5 = resolver.getDestinationForPath(
        "/readonly/folder0/folder0/file0.txt");
    assertDest("subcluster1", dest5);
    PathLocation dest6 = resolver.getDestinationForPath(
        "/readonly/folder0/folder0/file1.txt");
    assertDest("subcluster1", dest6);

    // All these must be in the same location: subcluster2
    PathLocation dest7 = resolver.getDestinationForPath("/readonly/folder1");
    assertDest("subcluster2", dest7);
    PathLocation dest8 = resolver.getDestinationForPath(
        "/readonly/folder1/file0.txt");
    assertDest("subcluster2", dest8);
    PathLocation dest9 = resolver.getDestinationForPath(
        "/readonly/folder1/file1.txt");
    assertDest("subcluster2", dest9);

    // All these must be in the same location: subcluster2
    PathLocation dest10 = resolver.getDestinationForPath("/readonly/folder2");
    assertDest("subcluster1", dest10);
    PathLocation dest11 = resolver.getDestinationForPath(
        "/readonly/folder2/file0.txt");
    assertDest("subcluster1", dest11);
    PathLocation dest12 = resolver.getDestinationForPath(
        "/readonly/folder2/file1.txt");
    assertDest("subcluster1", dest12);
  }

  @Test
  public void testLocalResolver() throws IOException {
    PathLocation dest0 =
        resolver.getDestinationForPath("/local/folder0/file0.txt");
    assertDest("subcluster0", dest0);
  }

  @Test
  public void testRandomResolver() throws IOException {
    Set<String> destinations = new HashSet<>();
    for (int i = 0; i < 30; i++) {
      PathLocation dest =
          resolver.getDestinationForPath("/random/folder0/file0.txt");
      RemoteLocation firstDest = dest.getDestinations().get(0);
      String nsId = firstDest.getNameserviceId();
      destinations.add(nsId);
    }
    assertEquals(3, destinations.size());
  }

  @Test
  public void testFixedResolver() throws IOException {
    PathLocation dest0 = resolver.getDestinationForPath("/fixed/folder0/file0.txt");
    // 2->1->0
    assertDest("subcluster2", dest0);
  }

  /**
   * Test that a path has files distributed across destinations evenly.
   * @param path Path to check.
   * @throws IOException
   */
  private void testEvenDistribution(final String path) throws IOException {
    testEvenDistribution(path, true);
  }

  /**
   * Test that a path has files distributed across destinations evenly or not.
   * @param path Path to check.
   * @param even If the distribution should be even or not.
   * @throws IOException If it cannot check it.
   */
  private void testEvenDistribution(final String path, final boolean even)
      throws IOException {

    // Subcluster -> Files
    Map<String, Set<String>> results = new HashMap<>();
    for (int f = 0; f < 10000; f++) {
      String filename = path + "/file" + f + ".txt";
      PathLocation destination = resolver.getDestinationForPath(filename);
      RemoteLocation loc = destination.getDefaultLocation();
      assertEquals(filename, loc.getDest());

      String nsId = loc.getNameserviceId();
      if (!results.containsKey(nsId)) {
        results.put(nsId, new TreeSet<>());
      }
      results.get(nsId).add(filename);
    }

    if (!even) {
      // All files should be in one subcluster
      assertEquals(1, results.size());
    } else {
      // Files should be distributed somewhat evenly
      assertEquals(3, results.size());
      int count = 0;
      for (Set<String> files : results.values()) {
        count = count + files.size();
      }
      int avg = count / results.keySet().size();
      for (Set<String> files : results.values()) {
        int filesCount = files.size();
        // Check that the count in each namespace is within 20% of avg
        assertTrue(filesCount > 0);
        assertTrue(Math.abs(filesCount - avg) < (avg / 5));
      }
    }
  }

  @Test
  public void testSuffixResolver() throws IOException {

    // /suffix
    PathLocation dest = resolver.getDestinationForPath("/suffix");
    assertEquals(5, dest.getDestinations().size());
    assertEquals(DestinationOrder.SUFFIX.name(), dest.getDestinationOrder().name());

    // /suffix/dir0/file0.txt
    PathLocation dest0 = resolver.getDestinationForPath("/suffix/dir0/file0.txt");
    assertDest("subcluster0", dest0);

    // /suffix/dir1/file1.txt
    PathLocation dest1 = resolver.getDestinationForPath("/suffix/dir1/file1.txt");
    assertDest("subcluster1", dest1);

    // /suffix/dir2/file2.txt
    PathLocation dest2 = resolver.getDestinationForPath("/suffix/dir2/file2.txt");
    assertDest("subcluster2", dest2);

    // point /suffix to subclusters 0, 1, 2 with the suffix order,
    // and point /dir3 to subclusters 3,
    // so point /suffix/dir3/file3.txt dest ns is subclusters 3.
    PathLocation dest3 = resolver.getDestinationForPath("/suffix/dir3/file3.txt");
    assertDest("subcluster3", dest3);

    // point /suffix to subclusters 0, 1, 2 with the suffix order,
    // point / to subclusters 0, 1, 2 with default order (hash), not set point /dir4,
    // and get point /dir4 dest ns is subcluster0, 1, 2,
    // so point /suffix/dir4/file4.txt dest ns also is subclusters 0, 1, 2.
    PathLocation dest4 = resolver.getDestinationForPath("/suffix/dir4/file4.txt");
    assertEquals(3, dest4.getDestinations().size());

    // point /suffix to subclusters 0, 1, 2 with the suffix order,
    // point /suffix/dir5 to subclusters 0, 1, 2 with the suffix order,
    // so point /suffix/dir5/file5.txt will throw RouterResolveException.
    assertThrows(RouterResolveException.class, () ->
        resolver.getDestinationForPath("/suffix/dir5/file5.txt"));

    // point /suffix to subclusters 6 and dstPath path is /suffix6 with the suffix order,
    // and point /dir6 to subclusters 6,
    // so point /suffix/dir6/file6.txt dest ns is subclusters 6
    // and dstPath is /suffix6/dir6/file6.txt.
    PathLocation dest6 = resolver.getDestinationForPath("/suffix/dir6/file6.txt");
    assertEquals(1, dest6.getDestinations().size());
    assertEquals("/suffix6/dir6/file6.txt", dest6.getDestinations().get(0).getDest());
    assertEquals("subcluster6", dest6.getDestinations().get(0).getNameserviceId());

    // point /suffix to subclusters 7 and dstPath path is /suffix7 with the suffix order,
    // and point /dir7 to subclusters 7 and dstPath path is /dir7_sub,
    // so point /suffix/dir7/file7.txt dest ns is subclusters 7
    // and dstPath is /suffix7/dir7_sub/file7.txt.
    PathLocation dest7 = resolver.getDestinationForPath("/suffix/dir7/file7.txt");
    assertEquals(1, dest7.getDestinations().size());
    assertEquals("/suffix7/dir7_sub/file7.txt", dest7.getDestinations().get(0).getDest());
    assertEquals("subcluster7", dest7.getDestinations().get(0).getNameserviceId());

    // add path suffix is "_COPYING_".
    dest7 = resolver.getDestinationForPath("/suffix/dir7/file7.txt_COPYING_");
    assertEquals(1, dest7.getDestinations().size());
    assertEquals("/suffix7/dir7_sub/file7.txt_COPYING_",
        dest7.getDestinations().get(0).getDest());
    assertEquals("subcluster7", dest7.getDestinations().get(0).getNameserviceId());
  }

  private static void assertDest(String expectedDest, PathLocation loc) {
    assertEquals(expectedDest, loc.getDestinations().get(0).getNameserviceId());
  }
}
