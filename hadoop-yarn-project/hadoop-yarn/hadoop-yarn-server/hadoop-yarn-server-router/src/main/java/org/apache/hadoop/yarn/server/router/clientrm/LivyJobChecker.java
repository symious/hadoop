package org.apache.hadoop.yarn.server.router.clientrm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.router.RouterServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Set;

public class LivyJobChecker implements SubmitAppChecker{

  private static final Logger LOG =
      LoggerFactory.getLogger(LivyJobChecker.class);

  private static final String LIVY_APP_TYPE = "SPARK";
  private static final String LIVY_TAG_HEADER = "livy:";
  private static final String ERROR_OUTPUT =
      "Your job can't submit by spark driver, please contact spark team " +
          "to switch livy!";

  private Collection<String> clusterIds;
  private String queueCheckMatchRegex;
  private Collection<String> queueCheckConfigList;
  private Collection<String> queueCheckIgnoreList;

  @Override
  public void check(String clusterId,
      ApplicationSubmissionContext applicationSubmissionContext)
      throws YarnException {

    boolean isPermit = false;

    String queue = applicationSubmissionContext.getQueue();
    Set<String> tags =
        applicationSubmissionContext.getApplicationTags();
    String jobType =
        applicationSubmissionContext.getApplicationType();

    //matched jobs pattern, eg: (AT0 + SPARK jobs + dev queues)
    if (clusterIds.contains(clusterId) && jobType.equals(LIVY_APP_TYPE) &&
        matchQueue(queue)) {

      //check matched jobs, only contain tag starts with "livy:" will permit
      if (tags != null && !tags.isEmpty()) {
        for (String tag : tags) {
          if (tag.startsWith(LIVY_TAG_HEADER)) {
            isPermit = true;
            break;
          }
        }
      }

    } else {
      // skip to check not matched jobs
      isPermit = true;
    }

    if (!isPermit) {
      RouterServerUtil.logAndThrowException(ERROR_OUTPUT, null);
    }
  }

  private boolean matchQueue(String queue) {
    if (queue == null) {
      queue = YarnConfiguration.DEFAULT_QUEUE_NAME;
    } else if (queue.startsWith("root.") && queue.length() > 5) {
      queue = queue.substring(5);
    }
    return (queue.matches(queueCheckMatchRegex) ||
        queueCheckConfigList.contains(queue)) &&
        !queueCheckIgnoreList.contains(queue);
  }

  @Override
  public void initialize(Configuration conf) {
    clusterIds = conf.getStringCollection(
        YarnConfiguration.ROUTER_SUBMIT_LIVY_JOB_CHECKER_CLUSTER_IDS);
    queueCheckMatchRegex = conf.get(
        YarnConfiguration.ROUTER_SUBMIT_LIVY_JOB_CHECKER_QUEUE_MATCH_REGEX,
        YarnConfiguration.DEFAULT_ROUTER_SUBMIT_LIVY_JOB_CHECKER_QUEUE_MATCH_REGEX);
    queueCheckConfigList = conf.getStringCollection(
        YarnConfiguration.ROUTER_SUBMIT_LIVY_JOB_CHECKER_QUEUE_CONFIG_LIST);
    queueCheckIgnoreList = conf.getStringCollection(
        YarnConfiguration.ROUTER_SUBMIT_LIVY_JOB_CHECKER_QUEUE_IGNORE_LIST);
    LOG.info("LivyJobChecker check clusterIds: " + clusterIds +
        " ,queueCheckMatchRegex: " + queueCheckMatchRegex +
        " ,queueCheckConfigList: " + queueCheckConfigList +
        " ,queueCheckIgnoreList: " + queueCheckIgnoreList);
  }

}
