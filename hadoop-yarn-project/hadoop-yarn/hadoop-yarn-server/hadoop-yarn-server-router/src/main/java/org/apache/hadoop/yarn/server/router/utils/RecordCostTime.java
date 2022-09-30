package org.apache.hadoop.yarn.server.router.utils;

public class RecordCostTime {
  private long recordTime;
  private long costTime;

  public RecordCostTime(long recordTime, long costTime) {
    this.recordTime = recordTime;
    this.costTime = costTime;
  }

  public long getRecordTime() {
    return recordTime;
  }

  public void setRecordTime(long recordTime) {
    this.recordTime = recordTime;
  }

  public long getCostTime() {
    return costTime;
  }

  public void setCostTime(long costTime) {
    this.costTime = costTime;
  }

  @Override
  public String toString() {
    return "RecordCostTime{" +
        "recordTime=" + recordTime +
        ", costTime=" + costTime +
        '}';
  }
}
