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

/**
 * Check if a record matches a query. The query is usually a partial record.
 */
public class Query<T extends BaseRecord> {

  /** Partial object to compare against. */
  private final T partial;


  /**
   * Create a query to search for a partial record.
   */
  public Query(final T part) {
    this.partial = part;
  }

  /**
   * Get the partial record used to query.
   */
  public T getPartial() {
    return this.partial;
  }

  /**
   * Check if a record matches the primary keys or the partial record.
   */
  public boolean matches(T other) {
    if (this.partial == null) {
      return false;
    }
    return this.partial.match(other);
  }

  /**
   * Check if the path of the query contains the path of other
   */
  public boolean likes(T other) {
    if (this.partial == null) {
      return false;
    }
    return this.partial.contain(other);
  }

  @Override
  public String toString() {
    return "Checking: " + this.partial;
  }
}
