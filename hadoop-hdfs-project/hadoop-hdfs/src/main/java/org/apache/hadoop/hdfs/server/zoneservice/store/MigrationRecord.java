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

public class MigrationRecord extends BaseRecord{
  private String ns;
  private String path;
  private String rule;
  private long dateCreated;
  private long dateModified;
  private static final String PATHSUFFIX = "/";

  public MigrationRecord(String ns, String path, String rule) {
    init();
    this.ns = ns;
    this.path = unifyPath(path);
    this.rule = rule;
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
    return ns + "_" + path;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof MigrationRecord)) {
      return false;
    }
    MigrationRecord record = (MigrationRecord) obj;
    return this.ns.equals(record.ns) && this.path.equals(record.path)
        && this.rule.equals(record.rule);
  }

  @Override
  public String toString() {
    return "MigrationRecord{" +
        "ns='" + ns + '\'' +
        ", path='" + path + '\'' +
        ", rule='" + rule + '\'' +
        ", dateCreated=" + dateCreated +
        ", dateModified=" + dateModified +
        '}';
  }

  //Remove the path suffix -- "/"
  private String unifyPath(String path) {
    if (path.endsWith(PATHSUFFIX)) {
      return path.substring(0, path.length() - 1);
    }
    return path;
  }
}
