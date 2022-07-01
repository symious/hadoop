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

package org.apache.hadoop.externaltrash;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Assert;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;

public class TestExternalTrash extends TestCase {

    private final static Path TEST_DIR =
            new Path(new File(System.getProperty("test.build.data","/tmp")
            ).toURI().toString().replace(' ', '+'), "testExternalTrash");

    private String origUserDir;

    static class TestLFS extends LocalFileSystem {
        Path home;
        TestLFS() {
            this(new Path("user/test"));
        }
        TestLFS(Path home) {
            super();
            this.home = home;
        }
        @Override
        public Path getHomeDirectory() {
            return home;
        }
    }

    @Override
    protected void setUp() throws Exception {
        origUserDir = System.getProperty("user.dir");
        System.setProperty("user.dir", TEST_DIR.toUri().getPath().toString());
    }

    protected static Path mkdir(FileSystem fs, Path p) throws IOException {
        assertTrue(fs.mkdirs(p));
        assertTrue(fs.exists(p));
        assertTrue(fs.getFileStatus(p).isDirectory());
        return p;
    }

    static String writeFile(FileSystem fileSys, Path name, int fileSize)
            throws IOException {
        final long seed = 0xDEADBEEFL;
        // Create and write a file that contains three blocks of data
        FSDataOutputStream stm = fileSys.create(name);
        byte[] buffer = new byte[fileSize];
        Random rand = new Random(seed);
        rand.nextBytes(buffer);
        stm.write(buffer);
        stm.close();
        return new String(buffer);
    }

    public void testExternalTrash() throws Exception {
        Configuration conf = new Configuration();
        // Trash with 12 second deletes and 6 seconds checkpoints
        conf.set(FS_TRASH_INTERVAL_KEY, "0.2"); // 12 seconds
        conf.setClass("fs.file.impl", TestLFS.class, FileSystem.class);
        conf.set(FS_TRASH_CHECKPOINT_INTERVAL_KEY, "0.1"); // 6 seconds
        FileSystem fs = FileSystem.getLocal(conf);
        conf.set("fs.defaultFS", fs.getUri().toString());

        ExternalTrash externalTrash = new ExternalTrash(conf);
        Thread trashTread = new Thread(externalTrash);
        trashTread.start();


        // First create a new directory with mkdirs
        Path myPath = new Path("test/mkdirs");
        mkdir(fs, myPath);

        // Create user A\B\C's home directory
        Path homeDir = fs.getHomeDirectory();
        Path trashDirA = new Path(homeDir.getParent(), "A/.Trash/Current");
        Path trashDirB = new Path(homeDir.getParent(), "B/.Trash/Current");
        Path trashDirC = new Path(homeDir.getParent(), "C/.Trash/Current");

        mkdir(fs, trashDirA);
        mkdir(fs, trashDirB);
        mkdir(fs, trashDirC);

        int fileIndex = 0;
        Set<String> checkpointsA = new HashSet<String>();
        Set<String> checkpointsB = new HashSet<String>();
        Set<String> checkpointsC = new HashSet<String>();
        while (true)  {
            // Create a file with a new name and remove to trash (mock)
            Path myFile = new Path( "test/mkdirs/myFile" + fileIndex++);
            writeFile(fs, myFile, 10);
            fs.rename(myFile, trashDirA);

            myFile = new Path( "test/mkdirs/myFile" + fileIndex++);
            writeFile(fs, myFile, 10);
            fs.rename(myFile, trashDirB);

            myFile = new Path( "test/mkdirs/myFile" + fileIndex++);
            writeFile(fs, myFile, 10);
            fs.rename(myFile, trashDirC);


            FileStatus filesA[] = fs.listStatus(trashDirA.getParent());
            // Scan files in .Trash and add them to set of checkpoints
            for (FileStatus file : filesA) {
                String fileName = file.getPath().getName();
                checkpointsA.add(fileName);
            }

            FileStatus filesB[] = fs.listStatus(trashDirB.getParent());
            // Scan files in .Trash and add them to set of checkpoints
            for (FileStatus file : filesB) {
                String fileName = file.getPath().getName();
                checkpointsB.add(fileName);
            }

            FileStatus filesC[] = fs.listStatus(trashDirC.getParent());
            // Scan files in .Trash and add them to set of checkpoints
            for (FileStatus file : filesC) {
                String fileName = file.getPath().getName();
                checkpointsC.add(fileName);
            }

            if (checkpointsB.size() == 6) {
                Assert.assertTrue(checkpointsA.size() == checkpointsB.size());
                Assert.assertTrue(checkpointsC.size() == checkpointsA.size());

                Assert.assertTrue(filesA.length < filesB.length);
                Assert.assertTrue(filesC.length < filesA.length);
                break;
            }

            Thread.sleep(5000);
        }
        trashTread.interrupt();
        trashTread.join();
    }

    /**
     * @see TestCase#tearDown()
     */
    @Override
    protected void tearDown() throws IOException {
        System.setProperty("user.dir", origUserDir);
        File trashDir = new File(TEST_DIR.toUri().getPath());
        if (trashDir.exists() && !FileUtil.fullyDelete(trashDir)) {
            throw new IOException("Cannot remove data directory: " + trashDir);
        }
    }

}
