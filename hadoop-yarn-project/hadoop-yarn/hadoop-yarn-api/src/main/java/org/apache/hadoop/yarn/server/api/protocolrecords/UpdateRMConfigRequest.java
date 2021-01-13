package org.apache.hadoop.yarn.server.api.protocolrecords;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.yarn.util.Records;


@Private
@Stable
public class UpdateRMConfigRequest {

  @Public
  @Stable
  public static UpdateRMConfigRequest newInstance(){
    UpdateRMConfigRequest request =
        Records.newRecord(UpdateRMConfigRequest.class);
    return request;
  }
}
