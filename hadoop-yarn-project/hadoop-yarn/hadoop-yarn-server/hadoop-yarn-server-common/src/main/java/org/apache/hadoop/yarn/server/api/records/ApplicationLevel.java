package org.apache.hadoop.yarn.server.api.records;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.util.Records;

public abstract class ApplicationLevel {

  public static ApplicationLevel newInstance() {
    return Records.newRecord(ApplicationLevel.class);
  }

  public static ApplicationLevel newInstance(String appId, String appLevel) {
    ApplicationLevel applicationLevel = Records.newRecord(ApplicationLevel.class);
    applicationLevel.setApplicationId(appId);
    applicationLevel.setApplicationLevel(appLevel);
    return applicationLevel;
  }

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public abstract String getApplicationId();

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public abstract void setApplicationId(String applicationId);

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public abstract String getApplicationLevel();

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public abstract void setApplicationLevel(String applicationLevel);


}
