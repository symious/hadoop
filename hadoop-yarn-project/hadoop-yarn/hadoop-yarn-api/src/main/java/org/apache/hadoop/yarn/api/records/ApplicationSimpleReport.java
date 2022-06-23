package org.apache.hadoop.yarn.api.records;
import org.apache.hadoop.yarn.util.Records;

public abstract class ApplicationSimpleReport {

  public static ApplicationSimpleReport newInstance(ApplicationId applicationId,
      YarnApplicationState state) {
    ApplicationSimpleReport report = Records.newRecord(ApplicationSimpleReport.class);
    report.setApplicationId(applicationId);
    report.setYarnApplicationState(state);
    return report;
  }

  public abstract ApplicationId getApplicationId();

  public abstract void setApplicationId(ApplicationId applicationId);

  public abstract YarnApplicationState getYarnApplicationState();

  public abstract void setYarnApplicationState(YarnApplicationState state);

}
