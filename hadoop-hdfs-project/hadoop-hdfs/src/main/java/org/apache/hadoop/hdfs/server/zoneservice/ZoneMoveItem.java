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

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * A class to record the number of replicas of a block
 * should be migrated from source data center to target data center.
 */
public class ZoneMoveItem {
  protected final String sourceDataCenter;
  protected final String targetDataCenter;
  protected final short num;

  /**
   * @param sourceDataCenter source data center
   * @param targetDataCenter target data center
   * @param num replicas to move
   */
  ZoneMoveItem(@Nonnull final String sourceDataCenter,
      @Nonnull final String targetDataCenter, final short num) {
    this.sourceDataCenter = sourceDataCenter;
    this.targetDataCenter = targetDataCenter;
    this.num = num;
  }

  public String getSourceDataCenter() {
    return sourceDataCenter;
  }

  public String getTargetDataCenter() {
    return targetDataCenter;
  }

  public short getNum() {
    return num;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ZoneMoveItem that = (ZoneMoveItem) o;
    return num == that.num && sourceDataCenter.equals(
        that.sourceDataCenter) && targetDataCenter.equals(that.targetDataCenter);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sourceDataCenter, targetDataCenter, num);
  }

  @Override
  public String toString() {
    return String.format("ZoneMoveItem{source='%s', target='%s', num=%d}",
        sourceDataCenter, targetDataCenter, num);
  }
}
