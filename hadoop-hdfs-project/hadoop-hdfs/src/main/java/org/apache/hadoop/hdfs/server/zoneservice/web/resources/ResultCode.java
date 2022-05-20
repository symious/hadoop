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


import org.json.JSONObject;

public enum ResultCode {
  SUCCESS(0, "Success!"),
  CREATE_SUCCESS(1, "Create Success!"),
  UPDATE_SUCCESS(2, "Update Success!"),

  IN_PROGRESS(1000, "In Process"),
  ALREADY_RUNNING(1001, "Already running"),
  NO_MOVE_BLOCK(1002, "No move block"),
  NO_MOVE_PROGRESS(1003, "No move progress"),
  NO_MIGRATION_RECORD(1004, "No migration record found"),
  NO_DISTRIBUTION(1005, "No distribution found for the give path"),
  IO_EXCEPTION(1006, "Fail caused by IOException!"),
  ILLEGAL_ARGUMENTS(1007, "Illegal arguments"),
  INTERRUPTED(1008, "Fail caused by interrupt"),
  UNFINALIZED_UPGRADE(1009, "Unfinalized Upgrade"),
  THREAD_FULL(1010, "No more available thread"),
  METHOD_ERROR(1011, "Cannot create new record"),
  REJECT(1012, "Reject! There has already been rule on the path!"),
  UNKNOWNERROR(1013, "Unknown ERROR!");

  private final Integer code;
  private final String msg;
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
    return (new JSONObject(this)).toString();
  }
}