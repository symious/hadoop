package org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.UpdateRMConfigRequestProto;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateRMConfigRequest;

@Private
@Unstable
public class UpdateRMConfigRequestPBImpl extends UpdateRMConfigRequest {
  UpdateRMConfigRequestProto proto = UpdateRMConfigRequestProto
      .getDefaultInstance();
  UpdateRMConfigRequestProto.Builder builder = null;
  boolean viaProto = false;

  public UpdateRMConfigRequestPBImpl() {
    builder = UpdateRMConfigRequestProto.newBuilder();
  }

  public UpdateRMConfigRequestPBImpl(
      UpdateRMConfigRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public UpdateRMConfigRequestProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null)
      return false;
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }
}
