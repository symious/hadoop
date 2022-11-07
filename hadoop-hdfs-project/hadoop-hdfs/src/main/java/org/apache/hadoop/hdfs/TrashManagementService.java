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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.thirdparty.com.google.common.base.Strings;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_CONSTRAINT_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_CONSTRAINT_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;
import static org.apache.hadoop.util.ExitUtil.terminate;

public class TrashManagementService {
  public static final Logger LOG = LoggerFactory.getLogger(TrashManagementService.class);
  private static final String USAGE = "Usage: Trash management service\n" +
      "Configuration for trash management service is $HADOOP_CONF_DIR/trash-service.xml\n";

  private static final String DFS_TRASH_NAMESERVICES_KEY = "dfs.trash.nameservices";
  private static final String CONF_FILENAME = "trash-service.xml";
  private static final String CONF_PREFIX = "fs.";
  private static final String CONF_DELETE_INTERVAL_SUFFIX = ".trash.interval";
  private static final String CONF_EMPTIER_INTERVAL_SUFFIX = ".trash.checkpoint.interval";
  private static final String CONF_CONSTRAINT_SUFFIX = ".trash.constraint";
  private final Map<String, Long> ns2DeleteInterval = new HashMap<>();
  private final Map<String, Long> ns2EmptierInterval = new HashMap<>();
  private final Map<String, Long> ns2Constraint = new HashMap<>();
  private static Long deleteIntervalDefault;
  private static Long emptierIntervalDefault;
  private static Long constraintDefault;
  private static final int MSECS_PER_MINUTE = 60*1000;
  private final ExecutorService trashThreadPool = Executors.newCachedThreadPool();

  static {
    // make sure only administrator can run this tool.
    System.setProperty("HADOOP_USER_NAME", "hdfs");
  }
  public TrashManagementService(Configuration conf) throws IOException {
    try {
      loadConfig();
      initialize(conf);
    } catch (IOException | HadoopIllegalArgumentException e) {
      this.stopAtException(e);
      throw e;
    }
  }

  private void initialize(Configuration conf) throws IOException {
    String[] validNs = conf.getStrings(DFS_TRASH_NAMESERVICES_KEY);
    for (String ns : validNs) {
      submitEmptierThread(conf, ns);
    }
  }

  /**
   * submit emptier task for specific namespace with customized interval and constraint
   */
  private void submitEmptierThread(Configuration conf, String ns) throws IOException {
    final Configuration copyConf = new Configuration(conf);
    copyConf.set(FS_DEFAULT_NAME_KEY, "hdfs://" + ns);

    // Set emptier interval for namespace
    Long emptierInterval = emptierIntervalDefault;
    if (ns2EmptierInterval.containsKey(ns)) {
      emptierInterval = ns2EmptierInterval.get(ns);
    }
    validInterval(emptierInterval);

    // Set delete interval for namespace
    long trashInterval = deleteIntervalDefault;
    if (ns2DeleteInterval.containsKey(ns)) {
      trashInterval = ns2DeleteInterval.get(ns);
    }
    validInterval(trashInterval);
    copyConf.setLong(FS_TRASH_INTERVAL_KEY, trashInterval);


    // Set trash constraint for namespace
    if (ns2Constraint.containsKey(ns)) {
      copyConf.setLong(FS_TRASH_CONSTRAINT_KEY, ns2Constraint.get(ns));
    } else {
      copyConf.setLong(FS_TRASH_CONSTRAINT_KEY, constraintDefault);
    }

    FileSystem fs = FileSystem.get(copyConf);
    trashThreadPool.submit(new Trash(fs, copyConf).
        getEmptier(copyConf, emptierInterval * MSECS_PER_MINUTE));
    LOG.info("Start external trash management thread for {} successfully. \n" +
            "External Trash configuration: Deletion interval = {} minutes, " +
            "Emptier interval = {} minutes, constraint = {}\n", ns,
        copyConf.getFloat(FS_TRASH_INTERVAL_KEY, FS_TRASH_INTERVAL_DEFAULT),
        emptierInterval, copyConf.getLong(FS_TRASH_CONSTRAINT_KEY, FS_TRASH_INTERVAL_DEFAULT));
  }

  /**
   * Loop configurations and collect username and its trash-interval.
   */
  private void loadConfig() {
    this.ns2DeleteInterval.clear();
    this.ns2EmptierInterval.clear();
    this.ns2Constraint.clear();

    Configuration trashConf = new Configuration(false);

    trashConf.addResource(CONF_FILENAME);
    deleteIntervalDefault = trashConf.getLong(FS_TRASH_INTERVAL_KEY, FS_TRASH_INTERVAL_DEFAULT);
    emptierIntervalDefault = trashConf.getLong(FS_TRASH_CHECKPOINT_INTERVAL_KEY,
        FS_TRASH_CHECKPOINT_INTERVAL_DEFAULT);
    constraintDefault = trashConf.getLong(FS_TRASH_CONSTRAINT_KEY, FS_TRASH_CONSTRAINT_DEFAULT);

    // load namespaces' trash interval & constraint config
    for (Map.Entry<String, String> entry : trashConf) {
      if (!entry.getKey().startsWith(CONF_PREFIX)) {
        continue;
      }

      if (entry.getKey().endsWith(CONF_DELETE_INTERVAL_SUFFIX)) {
        String ns = entry.getKey().replace(CONF_PREFIX, "")
            .replace(CONF_DELETE_INTERVAL_SUFFIX, "");
        if (Strings.isNullOrEmpty(ns)) {
          continue;
        }

        Long interval = Long.parseLong(entry.getValue());
        LOG.debug("{}'s trash delete interval is {}.", ns, interval);
        this.ns2DeleteInterval.put(ns, interval);
      } else if (entry.getKey().endsWith(CONF_CONSTRAINT_SUFFIX)) {
        String ns = entry.getKey().replace(CONF_PREFIX, "")
            .replace(CONF_CONSTRAINT_SUFFIX, "");
        if (Strings.isNullOrEmpty(ns)) {
          continue;
        }

        Long constraint = Long.parseLong(entry.getValue());
        LOG.debug("{}'s trash delete constraint is {}.", ns, constraint);
        this.ns2Constraint.put(ns, constraint);
      } else if (entry.getKey().endsWith(CONF_EMPTIER_INTERVAL_SUFFIX)) {
        String ns =
            entry.getKey().replace(CONF_PREFIX, "")
                .replace(CONF_EMPTIER_INTERVAL_SUFFIX, "");
        if (Strings.isNullOrEmpty(ns)) {
          continue;
        }

        Long interval = Long.parseLong(entry.getValue());
        LOG.debug("{}'s trash emptier interval is {}.", ns, interval);
        this.ns2EmptierInterval.put(ns, interval);
      }
    }
  }

  private void validInterval(long interval) throws IOException {
    if (interval < 0) {
      throw new IOException("Cannot start trash management service with negative interval."
          + " Set trash interval to a positive value.");
    }
  }

  public static void createTrashService(String[] argv, Configuration conf) throws IOException {
    LOG.info("createTrashManagementService {}.", Arrays.asList(argv));
    if (conf == null) {
      conf = new HdfsConfiguration();
    }

    DefaultMetricsSystem.initialize("TrashManagementService");
    new TrashManagementService(conf);
  }

  public void stop() {
    trashThreadPool.shutdown();
  }

  private void stopAtException(Exception e) {
    try {
      this.stop();
    } catch (Exception ex) {
      LOG.warn("Encountered exception when handling exception {}", e.getMessage(), ex);
    }
  }

  /**
   * Parse the arguments for commands
   *
   * @param args the argument to be parsed
   * @return true when the argument matches help option, false if not
   */
  private static boolean parseHelpArgument(String[] args) {
    final Options helpOptions = new Options();
    final Option helpOpt = new Option("h", "help", false,
        "get help information");
    helpOptions.addOption(helpOpt);

    if (args.length == 1) {
      try {
        CommandLineParser parser = new PosixParser();
        CommandLine cmdLine = parser.parse(helpOptions, args);
        if (cmdLine.hasOption(helpOpt.getOpt()) || cmdLine.hasOption(helpOpt.getLongOpt())) {
          // should print out the help information
          System.out.println(TrashManagementService.USAGE + "\n");
          ToolRunner.printGenericCommandUsage(System.out);
          return true;
        }
      } catch (ParseException pe) {
        return false;
      }
    }
    return false;
  }

  public static void main(String[] arg) {
    if (parseHelpArgument(arg)) {
      System.exit(0);
    }
    try {
      StringUtils.startupShutdownMessage(TrashManagementService.class, arg, LOG);
      createTrashService(arg, null);
    } catch (Throwable e) {
      LOG.error("Failed to start Trash Management Service.", e);
      terminate(1, e);
    }
  }
}
