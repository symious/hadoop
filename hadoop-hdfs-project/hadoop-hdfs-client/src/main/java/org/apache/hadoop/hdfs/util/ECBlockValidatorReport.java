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

package org.apache.hadoop.hdfs.util;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * EC File validator block report.
 */
@InterfaceAudience.Private
public class ECBlockValidatorReport implements Writable {

  /** the block group name, such as blk_xxx */
  private String blockGroup;
  private boolean isHealthy = false;
  /** the block group is under-erasure-coded */
  private boolean isUnder = false;
  /** list of other exception's internal blocks in a block group  */
  private List<Report> failedBlockReports = new ArrayList<>();
  /** list of compute parity data not match's internal blocks in a block group  */
  private List<Report> corruptBlockReports = new ArrayList<>();
  /** list of checksum exception's internal blocks in a block group  */
  private List<Report> checkSumFailedBlockReports = new ArrayList<>();
  private String message;

  public ECBlockValidatorReport() {}

  public ECBlockValidatorReport(String blockGroup) {
    this.blockGroup = blockGroup;
  }

  public ECBlockValidatorReport(String blockGroup, String message) {
    this.blockGroup = blockGroup;
    this.message = message;
  }

  public String getBlockGroup() {
    return blockGroup;
  }

  public boolean isHealthy() {
    return isHealthy;
  }

  public void setHealthy(boolean healthy) {
    isHealthy = healthy;
  }

  public boolean isUnder() {
    return isUnder;
  }

  public void setUnder(boolean under) {
    isUnder = under;
  }

  public void addFailedBlockReports(String message) {
    failedBlockReports.add(new Report(message));
  }

  public void addFailedBlockReports(String internalBlock, String datanodeInfo, String message) {
    failedBlockReports.add(new Report(blockGroup, internalBlock, datanodeInfo, message));
  }

  public void addCorruptBlockReports(String internalBlock, String datanodeInfo) {
    corruptBlockReports.add(new Report(blockGroup, internalBlock, datanodeInfo, null));
  }

  public void addCheckSumFailedBlockReports(String internalBlock, String datanodeInfo,
      String message) {
    checkSumFailedBlockReports.add(new Report(blockGroup, internalBlock, datanodeInfo, message));
  }

  public List<Report> failedBlockReports() {
    return Collections.unmodifiableList(failedBlockReports);
  }

  public List<Report> corruptBlockReports() {
    return Collections.unmodifiableList(corruptBlockReports);
  }

  public List<Report> checkSumFailedBlockReports() {
    return Collections.unmodifiableList(checkSumFailedBlockReports);
  }

  public boolean isFailed() {
    return failedBlockReports.size() > 0;
  }

  public boolean isCorrupt() {
    return corruptBlockReports.size() > 0;
  }

  public boolean isCheckSumFailed() {
    return checkSumFailedBlockReports.size() > 0;
  }

  public String getMessage() {
    return message;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, blockGroup);
    out.writeBoolean(isHealthy);
    out.writeBoolean(isUnder);

    if(failedBlockReports.size() > 0) {
      out.writeInt(failedBlockReports.size());
      for (Report report : failedBlockReports) {
        writeReport(out, report);
      }
    } else {
      out.writeInt(-1);
    }

    if(corruptBlockReports.size() > 0) {
      out.writeInt(corruptBlockReports.size());
      for (Report report : corruptBlockReports) {
        writeReport(out, report);
      }
    } else {
      out.writeInt(-1);
    }

    if(checkSumFailedBlockReports.size() > 0) {
      out.writeInt(checkSumFailedBlockReports.size());
      for (Report report : checkSumFailedBlockReports) {
        writeReport(out, report);
      }
    } else {
      out.writeInt(-1);
    }

    Text.writeString(out, message == null ? "N/A" : message);
  }

  private void writeReport(DataOutput out,Report report) throws IOException {
    Text.writeString(out, report.internalBlock == null ? "N/A" : report.internalBlock);
    Text.writeString(out, report.dataNodeInfo == null ? "N/A" : report.dataNodeInfo);
    Text.writeString(out, report.message == null ? "N/A" : report.message);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    blockGroup = Text.readString(in);
    isHealthy = in.readBoolean();
    isUnder = in.readBoolean();

    int failedBlockReportSize = in.readInt();
    if (failedBlockReportSize != -1) {
      failedBlockReports = Lists.newArrayListWithCapacity(failedBlockReportSize);
      for (int i = 0; i < failedBlockReportSize; i++) {
        failedBlockReports.add(new Report(blockGroup, Text.readString(in),
            Text.readString(in), Text.readString(in)));
      }
    } else {
      failedBlockReports = Lists.newArrayList();
    }

    int corruptBlockReportSize = in.readInt();
    if (corruptBlockReportSize != -1) {
      corruptBlockReports = Lists.newArrayListWithCapacity(corruptBlockReportSize);
      for (int i = 0; i < corruptBlockReportSize; i++) {
        corruptBlockReports.add(new Report(blockGroup, Text.readString(in),
            Text.readString(in), Text.readString(in)));
      }
    } else {
      corruptBlockReports = Lists.newArrayList();
    }

    int checkSumFailedBlockReportSize = in.readInt();
    if (checkSumFailedBlockReportSize != -1) {
      checkSumFailedBlockReports = Lists.newArrayListWithCapacity(checkSumFailedBlockReportSize);
      for (int i = 0; i < checkSumFailedBlockReportSize; i++) {
        checkSumFailedBlockReports.add(new Report(blockGroup, Text.readString(in),
            Text.readString(in), Text.readString(in)));
      }
    } else {
      checkSumFailedBlockReports = Lists.newArrayList();
    }

    message = Text.readString(in);
  }

  static class Report {
    private String blockGroup = null;
    private String internalBlock = null;
    private String dataNodeInfo = null;
    private String message = null;

    public Report(String message) {
      this.message = message;
    }

    public Report(String blockGroup, String internalBlock, String dataNodeInfo, String message) {
      this.internalBlock = internalBlock;
      this.dataNodeInfo = dataNodeInfo;
      this.message = message;
      this.blockGroup = blockGroup;
    }

    @Override
    public String toString() {
      StringBuilder str = new StringBuilder();
      if (blockGroup != null && !blockGroup.equals(ECFileValidator.EC_FILE_FAIL_BLOCK)) {
        str.append(blockGroup).append("$");
      }
      if (internalBlock != null) {
        str.append(internalBlock).append("$");
      }
      if (dataNodeInfo != null) {
        str.append(dataNodeInfo).append("$");
      }
      if (message != null) {
        str.append(message).append("$");
      }
      if (str.length() > 0) {
        str.deleteCharAt(str.length() - 1);
      }
      return str.toString();
    }
  }
}
