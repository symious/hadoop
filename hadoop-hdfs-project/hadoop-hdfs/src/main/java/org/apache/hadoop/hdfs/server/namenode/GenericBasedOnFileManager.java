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

import org.apache.commons.io.Charsets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Generic manager class based on file.
 * It has a set, and each line of the file is an element of the set.
 */
abstract class GenericBasedOnFileManager {

  private static final Log LOG = LogFactory.getLog(GenericBasedOnFileManager.class);

  private SortedSet<String> elementsSet = new TreeSet<>();

  protected void loadElements(File file) throws IOException {
    if (!file.exists()) {
      LOG.error(file + " not exists!");
      return;
    }

    // new set
    SortedSet<String> newLinesSet = new TreeSet<String>();
    LOG.info("Loading new lines from file: " + file);
    try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(new FileInputStream(file), Charsets.UTF_8))) {

      String line;
      while ((line = reader.readLine()) != null) {

        if (LOG.isDebugEnabled()) {
          LOG.debug("Loading new lines from file: Handle " + line);
        }

        // skip empty line and comment
        line = line.trim();
        if (line.isEmpty() || line.startsWith("#")) {
          continue;
        }

        newLinesSet.add(line);
      }
    }

    LOG.info("Loaded " + newLinesSet.size() + " from " + file);

    if (newLinesSet.size() == 0) {
      LOG.warn("Lines set is empty.");
    }
    // switch reference
    this.elementsSet = newLinesSet;
  }

  protected SortedSet<String> getElementsSet() {
    return this.elementsSet;
  }
}
