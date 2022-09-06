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
package org.apache.hadoop.hdfs.server.zoneservice.store;

import java.text.SimpleDateFormat;
import java.util.Date;

public class MigrationRecord extends BaseRecord{
  private String ns;
  private String path;
  private String rule;
  private String mode;
  private long dateCreated;
  private long dateModified;
  private static final String PATHSUFFIX = "/";
  private static final SimpleDateFormat dateFormat =
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  public MigrationRecord(String ns, String path, String rule) {
    this(ns, path, rule, "batch");
  }

  public MigrationRecord(String ns, String path, String rule, String mode) {
    init();
    this.ns = ns;
    this.path = unifyPath(path);
    this.rule = rule;
    this.mode = mode;
  }

  public void setNs(String ns) {
    this.ns = ns;
  }

  public String getNs() {
    return this.ns;
  }

  public void setPath(String path) {
    this.path = unifyPath(path);
  }

  public String getPath() {
    return this.path;
  }

  public void setRule(String rule) {
    this.rule = rule;
  }

  public String getRule() {
    return this.rule;
  }

  public void setMode(String mode) {
    this.mode = mode;
  }

  public String getMode() {
    return this.mode;
  }

  @Override
  public void setDateModified(long time) {
    this.dateModified = time;
  }

  @Override
  public long getDateModified() {
    return this.dateModified;
  }

  @Override
  public void setDateCreated(long time) {
    this.dateCreated = time;
  }

  @Override
  public long getDateCreated() {
    return this.dateCreated;
  }

  @Override
  public String getPrimaryKey() {
    return ns + "_" + mode + "_" + path;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof MigrationRecord)) {
      return false;
    }
    MigrationRecord record = (MigrationRecord) obj;
    return this.ns.equals(record.ns) && this.path.equals(record.path)
        && this.rule.equals(record.rule) && this.mode.equals(record.mode);
  }

  @Override
  public String toString() {
    return "MigrationRecord{" +
        "ns='" + ns + '\'' +
        ", path='" + path + '\'' +
        ", rule='" + rule + '\'' +
        ", mode='" + mode + '\'' +
        ", dateCreated=" + dateFormat.format(ms2Date(dateCreated)) +
        ", dateModified=" + dateFormat.format(ms2Date(dateModified)) +
        '}';
  }

  //Remove the path suffix -- "/"
  private String unifyPath(String path) {
    if (path.endsWith(PATHSUFFIX)) {
      return path.substring(0, path.length() - 1);
    }
    return path;
  }

  //Convert system ms to Date
  private Date ms2Date(Long currentMs) {
    Date date = new Date();
    date.setTime(currentMs);
    return date;
  }
}
