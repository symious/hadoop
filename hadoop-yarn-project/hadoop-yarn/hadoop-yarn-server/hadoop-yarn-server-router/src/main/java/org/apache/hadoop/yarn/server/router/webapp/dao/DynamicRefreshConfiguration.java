package org.apache.hadoop.yarn.server.router.webapp.dao;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "dynamicRefreshConfiguration")
@XmlAccessorType(XmlAccessType.FIELD)
public class DynamicRefreshConfiguration {
  private long getApplicationsMaxCostTime;
  private long getApplicationsRecordExpireTime;

  public DynamicRefreshConfiguration() {
  } // JAXB needs this

  public long getGetApplicationsMaxCostTime() {
    return getApplicationsMaxCostTime;
  }

  public void setGetApplicationsMaxCostTime(long getApplicationsMaxCostTime) {
    this.getApplicationsMaxCostTime = getApplicationsMaxCostTime;
  }

  public long getGetApplicationsRecordExpireTime() {
    return getApplicationsRecordExpireTime;
  }

  public void setGetApplicationsRecordExpireTime(
      long getApplicationsRecordExpireTime) {
    this.getApplicationsRecordExpireTime = getApplicationsRecordExpireTime;
  }

  @Override
  public String toString() {
    return "DynamicRefreshConfiguration{" +
        "getApplicationsMaxCostTime=" + getApplicationsMaxCostTime +
        ", getApplicationsRecordExpireTime=" + getApplicationsRecordExpireTime +
        '}';
  }
}
