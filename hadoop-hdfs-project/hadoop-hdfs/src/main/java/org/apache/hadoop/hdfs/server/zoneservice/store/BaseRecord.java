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

import org.apache.hadoop.util.Time;

/**
 * Abstract base of a data record in the Store. All Store records are
 * derived from this class. Data records are persisted in the data store and
 * are identified by their primary key.
 */
public abstract class BaseRecord implements Comparable<BaseRecord> {

  /**
   * Set the modification time for the record.
   */
  public abstract void setDateModified(long time);

  /**
   * Get the modification time for the record.
   */
  public abstract long getDateModified();

  /**
   * Set the creation time for the record.
   */
  public abstract void setDateCreated(long time);

  /**
   * Get the creation time for the record.
   */
  public abstract long getDateCreated();

  /**
   * Get the primary key name.
   */
  public abstract String getPrimaryKey();

  /**
   * Initialize the object.
   */
  public void init() {
    // Call this after the object has been constructed
    initDefaultTimes();
  }

  /**
   * Initialize default times.
   */
  private void initDefaultTimes() {
    long now = Time.now();
    this.setDateCreated(now);
    this.setDateModified(now);
  }

  /**
   * Check if this record matches a partial record.
   */
  public boolean like(BaseRecord other) {
    if (other == null) {
      return false;
    }
    return getPrimaryKey().equals(other.getPrimaryKey());
  }

  @Override
  public int hashCode() {
    return getPrimaryKey().hashCode();
  }

  @Override
  public int compareTo(BaseRecord record) {
    if (record == null) {
      return -1;
    }
    // Descending date order
    return (int) (record.getDateModified() - this.getDateModified());
  }

  @Override
  public String toString() {
    return getPrimaryKey();
  }
}