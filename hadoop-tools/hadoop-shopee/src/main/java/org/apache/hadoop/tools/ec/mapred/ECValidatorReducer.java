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
package org.apache.hadoop.tools.ec.mapred;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.util.ECBlockValidatorReport;
import org.apache.hadoop.hdfs.util.ECFileValidator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Reducer class that executes print the ec file validate result operation.
 * Implements the Reducer interface.
 */
public class ECValidatorReducer extends Reducer<Text, ECBlockValidatorReport, NullWritable, Text> {

  private static Logger LOG = LoggerFactory.getLogger(ECValidatorReducer.class);

  private Text failed = new Text("failed");
  private Text healthy = new Text("healthy");
  private String SEPARATOR;
  private String STRING_PLACEHOLDER = "N/A";

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    Configuration conf = context.getConfiguration();
    SEPARATOR = conf.get(HdfsClientConfigKeys.DFS_EC_VALIDATOR_FIELD_SEPARATOR_KEY,
        HdfsClientConfigKeys.DFS_EC_VALIDATOR_FIELD_SEPARATOR_DEFAULT);
  }

  @Override
  protected void reduce(Text text, Iterable<ECBlockValidatorReport> values, Context context)
      throws IOException, InterruptedException {
    // Output in the format defined below.
    // ns|file|status(healthy or failed)|failed block groups|corrupt block groups|
    // under-erasure-coded block groups.
    // failed block groups is
    //   block_group_id$block_internal_id1$datanodeip:xferPort$message,block_group_id$
    //   block_internal_id2$datanodeip:xferPort$message
    // corrupt block groups is
    //   block_group_id$block_internal_id1$datanodeip:xferPort$N/A,block_group_id$
    //   block_internal_id2$datanodeip:xferPort$N/A
    // under-erasure-coded block groups is
    //   block_group_id$block_internal_id1$datanodeip:xferPort$N/A,block_group_id$
    //   block_internal_id2$datanodeip:xferPort$N/A

    List<String> failedReports = new ArrayList<>();
    List<String> underReports = new ArrayList<>();
    List<String> corruptReports = new ArrayList<>();

    boolean fileFailure = false;
    boolean isHealthy = true;
    String fileStr = text.toString();
    String fileStrS[] = fileStr.split("\t");
    String ns = "no ns";
    String file = fileStr;
    if (fileStrS.length == 2) {
      ns = fileStrS[0];
      file = fileStrS[1];
    }

    for (ECBlockValidatorReport report : values) {
      if (report.getBlockGroup().equals(ECFileValidator.EC_FILE_FAIL_BLOCK)) {
        fileFailure = true;
        isHealthy = false;
        failedReports.add(report.getMessage());
        break;
      }

      if (report.isHealthy()) {
        continue;
      }

      if (report.isUnder()) {
        isHealthy = false;
        underReports.add(report.getBlockGroup());
      } else if (report.isCorrupt()) {
        isHealthy = false;
        corruptReports.add(report.corruptBlockReports().toString());
      } else {
        isHealthy = false;
        if (report.isCheckSumFailed()) {
          failedReports.add(report.checkSumFailedBlockReports().toString());
        }
        if (report.isFailed()) {
          failedReports.add(report.failedBlockReports().toString());
        }
      }
    }

    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(ns).append(SEPARATOR);
    stringBuilder.append(file).append(SEPARATOR);
    if (fileFailure) {
      String msg = failedReports.get(0);
      LOG.info("Checking EC File: {} status is failed and {}.", file, msg);
      appendMessage(stringBuilder, failed.toString(), msg, STRING_PLACEHOLDER, STRING_PLACEHOLDER);
      context.write(NullWritable.get(), new Text(stringBuilder.toString()));
      return;
    }

    if (isHealthy) {
      LOG.info("Checking EC File: {} status is ok.", file);
      appendMessage(stringBuilder, healthy.toString(), STRING_PLACEHOLDER, STRING_PLACEHOLDER,
          STRING_PLACEHOLDER);
      context.write(NullWritable.get(), new Text(stringBuilder.toString()));
      return;
    }

    LOG.info("Checking EC File: {} status is failed.", file);
    stringBuilder.append(failed.toString());
    appendBlocksWithMessage(stringBuilder, failedReports);
    appendBlocksWithMessage(stringBuilder, corruptReports);
    appendBlocksWithMessage(stringBuilder, underReports);
    context.write(NullWritable.get(), new Text(stringBuilder.toString()));
  }

  private void appendBlocksWithMessage(StringBuilder stringBuilder,
      List<String> reports) {
    if (stringBuilder.length() > 0) {
      stringBuilder.append(this.SEPARATOR);
    }

    if (reports.size() == 0) {
      stringBuilder.append(STRING_PLACEHOLDER);
    }

    for (String report : reports) {
      stringBuilder.append(StringUtils.join(report, ","));
    }
  }

  private void appendMessage(StringBuilder stringBuilder, String status, String failedMsg,
      String corruptMsg, String underMsg) {
    stringBuilder.append(status).append(SEPARATOR).append(failedMsg).append(SEPARATOR).
        append(corruptMsg).append(SEPARATOR).append(underMsg);
  }
}
