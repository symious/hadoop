package org.apache.hadoop.yarn.server.router.clientrm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.exceptions.YarnException;

public interface SubmitAppChecker{

  void check(String clusterId,
      ApplicationSubmissionContext applicationSubmissionContext)
      throws YarnException;

  void initialize(Configuration conf);
}
