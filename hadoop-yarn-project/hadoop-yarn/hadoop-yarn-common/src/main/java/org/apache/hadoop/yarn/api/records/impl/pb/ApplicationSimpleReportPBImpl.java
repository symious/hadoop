package org.apache.hadoop.yarn.api.records.impl.pb;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSimpleReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationSimpleReportProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationSimpleReportProtoOrBuilder;

public class ApplicationSimpleReportPBImpl extends ApplicationSimpleReport {
  ApplicationSimpleReportProto proto =
      ApplicationSimpleReportProto.getDefaultInstance();
  ApplicationSimpleReportProto.Builder builder = null;

  boolean viaProto = false;

  private ApplicationId applicationId;

  public ApplicationSimpleReportPBImpl() {
    builder = ApplicationSimpleReportProto.newBuilder();
  }

  public ApplicationSimpleReportPBImpl(ApplicationSimpleReportProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public ApplicationId getApplicationId() {
    if (this.applicationId != null) {
      return this.applicationId;
    }

    ApplicationSimpleReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasApplicationId()) {
      return null;
    }
    this.applicationId = convertFromProtoFormat(p.getApplicationId());
    return applicationId;
  }

  @Override
  public void setApplicationId(ApplicationId applicationId) {
    maybeInitBuilder();
    if (applicationId == null)
      builder.clearApplicationId();
    this.applicationId = applicationId;
  }

  @Override
  public YarnApplicationState getYarnApplicationState() {
    ApplicationSimpleReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasYarnApplicationState()) {
      return null;
    }
    return convertFromProtoFormat(p.getYarnApplicationState());
  }

  @Override
  public void setYarnApplicationState(YarnApplicationState state) {
    maybeInitBuilder();
    if (state == null) {
      builder.clearYarnApplicationState();
      return;
    }
    builder.setYarnApplicationState(convertToProtoFormat(state));
  }

  private void mergeLocalToBuilder() {
    if (this.applicationId != null
        && !((ApplicationIdPBImpl) this.applicationId).getProto()
        .equals(builder.getApplicationId())) {
      builder.setApplicationId(convertToProtoFormat(applicationId));
    }
  }

  private void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ApplicationSimpleReportProto.newBuilder(proto);
    }
    viaProto = false;
  }

  public ApplicationSimpleReportProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private ApplicationIdPBImpl convertFromProtoFormat(
      YarnProtos.ApplicationIdProto applicationId) {
    return new ApplicationIdPBImpl(applicationId);
  }

  private YarnApplicationState convertFromProtoFormat(
      YarnProtos.YarnApplicationStateProto s) {
    return ProtoUtils.convertFromProtoFormat(s);
  }

  private YarnProtos.ApplicationIdProto convertToProtoFormat(ApplicationId t) {
    return ((ApplicationIdPBImpl) t).getProto();
  }

  private YarnProtos.YarnApplicationStateProto convertToProtoFormat(YarnApplicationState s) {
    return ProtoUtils.convertToProtoFormat(s);
  }
}
