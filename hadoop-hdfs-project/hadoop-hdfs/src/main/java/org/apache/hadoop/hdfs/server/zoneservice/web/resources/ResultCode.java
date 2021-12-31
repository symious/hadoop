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
package org.apache.hadoop.hdfs.server.zoneservice.web.resources;

enum ResultCode {
  SUCCESS(0, "Success!"),
  MONITOR_MODE_ON(1, "Monitor Mode ON"),
  MONITOR_MODE_OFF(2, "Monitor Mode OFF"),
  NOT_FOUND(3, "The thread not found"),

  IN_PROGRESS(1000, "In Process"),
  ALREADY_RUNNING(1001, "Already running"),
  NO_MOVE_BLOCK(1002, "No move block"),
  NO_MOVE_PROGRESS(1003, "No move progress"),
  IO_EXCEPTION(1004, "Fail caused by IOException!"),
  ILLEGAL_ARGUMENTS(1005, "Illegal arguments"),
  INTERRUPTED(1006, "Fail caused by interrupt"),
  UNFINALIZED_UPGRADE(1007, "Unfinalized Upgrade"),
  ZONEMOVERINEXISTERROR(1008, "No running ZoneMover found"),
  UNKNOWNERROR(1009, "Unknown ERROR!");

  private Integer code;
  private String msg;
  ResultCode(Integer code, String msg) {
    this.code = code;
    this.msg = msg;
  }

  public Integer getCode() {
    return code;
  }

  public String getMsg() {
    return msg;
  }

  public String toString() {
    return "code=" + code + ", massage=" + msg;
  }
}
