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

package org.apache.hadoop.hdfs.server.zoneservice;

public class MoverPathInfo  {
  private final int partition;
  private final long recordOffset;
  private final String fullPath;
  private final long fileId;

  public MoverPathInfo(int partition, long recordOffset, String fullPath, long fileId) {
    this.partition = partition;
    this.recordOffset = recordOffset;
    this.fullPath = fullPath;
    this.fileId = fileId;
  }

  public int getPartition() {
    return partition;
  }

  public long getRecordOffset() {
    return recordOffset;
  }

  public String getFullPath() {
    return fullPath;
  }

  public long getFileId() {
    return fileId;
  }
}