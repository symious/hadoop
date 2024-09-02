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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import static org.apache.hadoop.io.IOUtils.readFullyToByteArray;

public class MigrationUtils {
  public static Set<Path> loadPaths(Path path, String inputFile) throws IOException {
    Set<Path> paths = new HashSet<>();
    if (inputFile == null) {
      paths.add(path);
    } else {
      File input = new File(inputFile);
      if (!input.exists()) {
        return paths;
      }
      BufferedReader reader =
          new BufferedReader(new InputStreamReader(Files.newInputStream(input.toPath())));
      String line;
      while ((line = reader.readLine()) != null) {
        paths.add(new Path(line.trim()));
      }
    }
    return paths;
  }

  public static Set<Path> loadPathsFromDfs(DistributedFileSystem dfs, Path path, Path inputFilePath)
      throws IOException {
    Set<Path> paths = new HashSet<>();
    if (inputFilePath == null) {
      paths.add(path);
    } else {
      FSDataInputStream is;
      try {
        is = dfs.open(inputFilePath);
      } catch (FileNotFoundException fnfe) {
        return paths;
      }
      byte[] fullBytes = readFullyToByteArray(is);
      is.close();
      String fullString = new String(fullBytes);
      for (String split : fullString.split("\n")) {
        paths.add(new Path(split));
      }
    }
    return paths;
  }

  public static void appendLineToFileInDfs(DistributedFileSystem srcFs, String line, Path output) {
    try (FSDataOutputStream os = srcFs.append(output)) {
      os.writeBytes(line + "\n");
    } catch (FileNotFoundException fnfe) {
      try (FSDataOutputStream os = srcFs.create(output)) {
        os.writeBytes(line + "\n");
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
