package org.apache.hadoop.hdfs.server.zoneservice;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

public class AuditLogger {
  private static final Log LOG = LogFactory.getLog(AuditLogger.class);

  enum Keys {METHOD, NAMESPACE, PATH, RULE, STARTTIME, ENDTIME,
    STATUS, MODE, IP}

  static class Constants {
    static final String KEY_VAL_SEPARATOR = "=";
    static final char PAIR_SEPARATOR = '\t';
    static final SimpleDateFormat formatter =
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  }

  //Generate audit log for apply rule process
  public static void logRuleProcess(String methodName, String nameSpace,
      String path, String rule, Date startTime, Date endTime,
      String status, String mode) {
    if (LOG.isInfoEnabled()) {
      LOG.info(createLog(methodName, nameSpace, path, rule, startTime, endTime,
          status, mode));
    }
  }

  static String createLog(String methodName, String nameSpace, String path,
      String rule, Date startTime, Date endTime, String status, String mode) {
    StringBuilder b = new StringBuilder();
    start(Keys.METHOD, methodName, b);
    add(Keys.NAMESPACE, nameSpace, b);
    add(Keys.PATH, path, b);
    add(Keys.RULE, rule, b);
    add(Keys.STARTTIME, Constants.formatter.format(startTime), b);
    add(Keys.ENDTIME, Constants.formatter.format(endTime), b);
    add(Keys.STATUS, status, b);
    add(Keys.MODE, mode, b);
    return b.toString();
  }

  /**
   * Adds the first key-val pair to the passed builder in the following format
   * key=value
   */
  static void start(Keys key, String value, StringBuilder b) {
    b.append(key.name()).append(Constants.KEY_VAL_SEPARATOR).append(value);
  }

  /**
   * Appends the key-val pair to the passed builder in the following format
   * <pair-delim>key=value
   */
  static void add(Keys key, String value, StringBuilder b) {
    b.append(Constants.PAIR_SEPARATOR).append(key.name())
        .append(Constants.KEY_VAL_SEPARATOR).append(value);
  }
}
