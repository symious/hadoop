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

public class ZoneServiceHttpResponse {
  private final ResultCode status;
  private final String data;

  public ZoneServiceHttpResponse(String dataInput) {
    status = ResultCode.SUCCESS;
    data = dataInput;
  }

  public ZoneServiceHttpResponse(ResultCode resultCode, String dataInput) {
    status = resultCode;
    data = dataInput;
  }


  public ZoneServiceHttpResponse(ResultCode resultCode) {
    status = resultCode;
    data = "{}";
  }

  @Override
  public String toString() {
    return status.toString().substring(0, status.toString().length()-1) +
        ",\"data\":" + data + "}";
  }
}