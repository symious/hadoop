package org.apache.hadoop.yarn.server.api.records.impl.pb;

import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos;
import org.apache.hadoop.yarn.server.api.records.ApplicationLevel;

public class ApplicationLevelPBImpl extends ApplicationLevel {
  private YarnServerCommonServiceProtos.ApplicationLevelProto proto =
      YarnServerCommonServiceProtos.ApplicationLevelProto
          .getDefaultInstance();
  private YarnServerCommonServiceProtos.ApplicationLevelProto.Builder
      builder = null;
  private boolean viaProto = false;

  public ApplicationLevelPBImpl() {
    builder =
        YarnServerCommonServiceProtos.ApplicationLevelProto.newBuilder();
  }

  public ApplicationLevelPBImpl(YarnServerCommonServiceProtos
      .ApplicationLevelProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public YarnServerCommonServiceProtos.ApplicationLevelProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = YarnServerCommonServiceProtos.ApplicationLevelProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public String getApplicationId() {
    YarnServerCommonServiceProtos.ApplicationLevelProto p =
        viaProto ? proto : builder.build();
    return p.getApplicationId();
  }

  @Override
  public void setApplicationId(String applicationId) {
    maybeInitBuilder();
    builder.setApplicationId(applicationId);
  }

  @Override
  public String getApplicationLevel() {
    YarnServerCommonServiceProtos.ApplicationLevelProto p =
        viaProto ? proto : builder.build();
    return p.getApplicationLevel();
  }

  @Override
  public void setApplicationLevel(String applicationLevel) {
    maybeInitBuilder();
    builder.setApplicationLevel(applicationLevel);
  }
}
