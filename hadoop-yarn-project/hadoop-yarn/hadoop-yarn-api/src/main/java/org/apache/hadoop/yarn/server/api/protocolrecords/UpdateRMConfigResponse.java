package org.apache.hadoop.yarn.server.api.protocolrecords;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.yarn.util.Records;


@Private
@Evolving
public class UpdateRMConfigResponse {

  @Private
  @Stable
  public static UpdateRMConfigResponse newInstance() {
    UpdateRMConfigResponse response =
        Records.newRecord(UpdateRMConfigResponse.class);
    return response;
  }
}
