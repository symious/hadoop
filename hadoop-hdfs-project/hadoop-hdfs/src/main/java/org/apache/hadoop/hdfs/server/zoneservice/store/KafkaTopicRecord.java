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

public class KafkaTopicRecord extends BaseRecord {
  private final String ns;
  private final String topic;
  private final String groupId;
  private final int partition;
  private final long offset;
  private long dateCreated;
  private long dateModified;
  private static final SimpleDateFormat dateFormat =
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  public KafkaTopicRecord(String ns, String topic, String groupId, int partition, long offset) {
    init();
    this.ns = ns;
    this.topic = topic;
    this.groupId = groupId;
    this.partition = partition;
    this.offset = offset;
  }

  public String getNs() {
    return ns;
  }

  public String getTopic() {
    return topic;
  }

  public String getGroupId() {
    return groupId;
  }

  public int getPartition() {
    return partition;
  }

  public long getOffset() {
    return offset;
  }

  @Override
  public void setDateModified(long time) {
    dateModified = time;
  }

  @Override
  public long getDateModified() {
    return dateModified;
  }

  @Override
  public void setDateCreated(long time) {
    dateCreated = time;
  }

  @Override
  public long getDateCreated() {
    return dateCreated;
  }

  @Override
  public String getPrimaryKey() {
    return ns + "_" + topic + "_" + groupId + "_" + partition;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof KafkaTopicRecord)) {
      return false;
    }
    KafkaTopicRecord record = (KafkaTopicRecord) obj;
    return this.ns.equals(record.ns) && this.topic.equals(record.topic)
        && this.groupId.equals(record.groupId) && this.partition == record.partition;
  }

  @Override
  public String toString() {
    return "KafkaTopicRecord{" +
        "ns='" + ns + '\'' +
        ", topic=" + topic + '\'' +
        ", groupId=" + groupId + '\'' +
        ", partition=" + partition + '\'' +
        ", offset=" + offset + '\'' +
        ", dateCreated=" + dateFormat.format(ms2Date(dateCreated)) +
        ", dateModified=" + dateFormat.format(ms2Date(dateModified)) +
        '}';
  }

  //Convert system ms to Date
  private Date ms2Date(Long currentMs) {
    Date date = new Date();
    date.setTime(currentMs);
    return date;
  }
}