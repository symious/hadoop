package org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateRMConfigResponse;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.UpdateRMConfigResponseProto;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

@Private
@Unstable
public class UpdateRMConfigResponsePBImpl extends UpdateRMConfigResponse {
  UpdateRMConfigResponseProto proto = YarnServerResourceManagerServiceProtos.UpdateRMConfigResponseProto
      .getDefaultInstance();
  UpdateRMConfigResponseProto.Builder builder = null;
  boolean viaProto = false;

  public UpdateRMConfigResponsePBImpl() {
    builder = UpdateRMConfigResponseProto.newBuilder();
  }

  public UpdateRMConfigResponsePBImpl(
      UpdateRMConfigResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public UpdateRMConfigResponseProto getProto() {
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
