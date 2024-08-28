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

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NSMigrationTool extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory.getLogger(NSMigrationTool.class);

  public NSMigrationTool(Configuration conf) {
    super(conf);
  }

  public static void main(String[] argv) {
    Configuration conf = new Configuration();
    NSMigrationTool tool = new NSMigrationTool(conf);
    int exitCode;
    try {
      exitCode = ToolRunner.run(tool, argv);
    } catch (Exception e) {
      LOG.error("Failed to run MigrationTool", e);
      exitCode = -1;
    }
    System.exit(exitCode);
  }

  @Override
  public int run(String[] args) throws Exception {
    List<String> argsList = new LinkedList<>(Arrays.asList(args));
    String command = argsList.remove(0);
    switch (command) {
      case "migrate":
        return MigrationJob.handleArgs(argsList, getConf());
      case "createTopDir":
        return CreateTopDirJob.handleArgs(argsList, getConf());
      case "analyze":
        return AnalyzeJob.handleArgs(argsList, getConf());
      case "project":
        return ProjectJob.handleArgs(argsList, getConf());
      default:
        System.err.println("Only migrate or createTopDir commands are allowed.");
        return -1;
    }
  }
}
