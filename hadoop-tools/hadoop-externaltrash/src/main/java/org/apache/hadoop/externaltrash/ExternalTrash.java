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

import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.*;

/**
 * A external trash utility to create and delete checkpoints of different user's trash
 * with different interval.
 */
public class ExternalTrash implements Runnable {

  private static final Log LOG = LogFactory.getLog(ExternalTrash.class);

  private static final String CONF_FILENAME = "externaltrash.xml";
  private static final String CONF_PREFIX = "fs.user.";
  private static final String CONF_SUFFIX = ".trash.interval";

  private static final String EXTERNAL_TRASH_ENABLE_KEY = "fs.user.trash.interval.enable";
  private static final boolean EXTERNAL_TRASH_ENABLE_DEFAULT = false;

  private static final String FS_TRASH_CHECKPOINT_INTERVAL_DELTA_KEY = "fs.trash.checkpoint.interval.delta";
  private static final float FS_TRASH_CHECKPOINT_INTERVAL_DELTA_DEFAULT = 60; // one hour

  private static final int MSECS_PER_MINUTE = 60*1000;

  private final FileSystem fs;
  private final Configuration conf;

  private long emptierInterval;

  private long emptierIntervalDelta; // avoid doing checkpoint at the same time with NameNode.

  private boolean enabled = false;

  private Map<String, Float> user2Interval = new HashMap<>();

  static {
    // make sure only administrator can run this tool.
    System.setProperty("HADOOP_USER_NAME", "hadoop");
  }

  public ExternalTrash(Configuration conf) throws IOException {
    this.conf = conf;
    fs = FileSystem.get(this.conf);

    long deletionInterval = (long)(conf.getFloat(
        FS_TRASH_INTERVAL_KEY, FS_TRASH_INTERVAL_DEFAULT)
        * MSECS_PER_MINUTE);
    this.emptierInterval = (long)(conf.getFloat(
        FS_TRASH_CHECKPOINT_INTERVAL_KEY, FS_TRASH_CHECKPOINT_INTERVAL_DEFAULT)
        * MSECS_PER_MINUTE);
    LOG.info("External Trash configuration: Deletion interval = " +
        (deletionInterval / MSECS_PER_MINUTE) + " minutes, Emptier interval = " +
        (this.emptierInterval / MSECS_PER_MINUTE) + " minutes.");

    if (this.emptierInterval <= 0 || this.emptierInterval > deletionInterval) {
      throw new IllegalArgumentException(FS_TRASH_CHECKPOINT_INTERVAL_KEY + "'s value is illegal.");
    }
  }

  /**
   * Loop configurations and collect username and its trash-interval.
   */
  private void reloadConfig() {

    this.user2Interval.clear();

    Configuration trashConf = new Configuration(false);
    trashConf.addResource(CONF_FILENAME);

    this.enabled = trashConf.getBoolean(EXTERNAL_TRASH_ENABLE_KEY, EXTERNAL_TRASH_ENABLE_DEFAULT);
    LOG.info("External Trash enabled = " + this.enabled);

    this.emptierIntervalDelta = (long)(trashConf.getFloat(
        FS_TRASH_CHECKPOINT_INTERVAL_DELTA_KEY, FS_TRASH_CHECKPOINT_INTERVAL_DELTA_DEFAULT)
        * MSECS_PER_MINUTE);
    LOG.info("External Trash interval delta = " + (this.emptierIntervalDelta / MSECS_PER_MINUTE) + " minutes.");

    // load user' trash interval config
    for (Map.Entry<String, String> entry : trashConf) {
      if (!entry.getKey().startsWith(CONF_PREFIX) ||
          !entry.getKey().endsWith(CONF_SUFFIX)) {
        continue;
      }

      String user = entry.getKey().replace(CONF_PREFIX, "").replace(CONF_SUFFIX, "");
      if (Strings.isNullOrEmpty(user)) {
        continue;
      }

      float interval = Float.parseFloat(entry.getValue());
      LOG.info(user + "'s trash interval is " + interval);
      this.user2Interval.put(user, interval);
    }

  }

  /**
   * Scan home's parent directory and process user's trash one by one.
   */
  @Override
  public void run() {

    LOG.info("ExternalTrashThread started.");

    //use simple path
    Path homesParent = new Path(fs.getHomeDirectory().toUri().getPath()).getParent();
    String rootTrash = "/Trash";

    Path trashHomeParent =  new Path(rootTrash + homesParent.toString());

    reloadConfig();

    while (true) {
      long now = Time.now();
      long end = ceiling(now, emptierInterval) + this.emptierIntervalDelta;
      try {                                     // sleep for interval
        Thread.sleep(end - now);
      } catch (InterruptedException e) {
        break;                                  // exit on interrupt
      }

      // reload configuration every time, so we can reconfiguration without restart daemon.
      reloadConfig();

      try {
        now = Time.now();
        if (now >= end) {

          FileStatus[] trashHomes = null;
          try {
            trashHomes = fs.listStatus(trashHomeParent);         // list all home dirs
          } catch (IOException e) {
            LOG.warn("Trash can't list homes: "+e+" Sleeping.");
            continue;
          }

          for (FileStatus trashHome : trashHomes) {         // dump each trash
            if (!trashHome.isDirectory())
              continue;

            // update trash interval if necessary
            Configuration copyConf = new Configuration(this.conf);
            String user = trashHome.getPath().getName();
            if (this.user2Interval.containsKey(user)) {
              copyConf.setFloat(FS_TRASH_INTERVAL_KEY, this.user2Interval.get(user));
            }

            LOG.info("Processing " + trashHome.getPath().toUri().getPath() + "'s trash. " +
                "Deletion interval(minutes) = " + copyConf.get(FS_TRASH_INTERVAL_KEY));

            if (!this.enabled) {
              continue;
            }

            try {
              TrashPolicy trash = TrashPolicy.getInstance(copyConf, fs,
                  new Path(trashHome.getPath().toString().replaceFirst(rootTrash,"")));
              trash.deleteCheckpoint();
              trash.createCheckpoint();
            } catch (IOException e) {
              LOG.warn("ExternalTrash caught: "+e+". Skipping "+trashHome.getPath()+".");
            }
          }
        }
      } catch (Exception e) {
        LOG.warn("RuntimeException during ExternalTrash.run(): ", e);
      }
    }
    try {
      fs.close();
    } catch(IOException e) {
      LOG.warn("ExternalTrash cannot close FileSystem: ", e);
    }

  }

  private long ceiling(long time, long interval) {
    return floor(time, interval) + interval;
  }
  private long floor(long time, long interval) {
    return (time / interval) * interval;
  }

  public static void main(String[] args) {

    StringUtils.startupShutdownMessage(ExternalTrash.class, args, LOG);

    if (ExternalTrash.class.getClassLoader().getResource(CONF_FILENAME) == null) {
      LOG.error("can't find " + CONF_FILENAME);
      return;
    }

    try {
      ExternalTrash externalTrash = new ExternalTrash(new Configuration());

      Thread externalTrashThread = new Thread(externalTrash, "External Trash Thread");
      externalTrashThread.setDaemon(true);
      externalTrashThread.start();

      externalTrashThread.join();
    } catch (Exception e) {
      LOG.fatal("Caught Exception: " + StringUtils.stringifyException(e));
    }

  }


}
