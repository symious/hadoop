package org.apache.hadoop.util;

public class Check {

  /**
   * Verifies a variable is not NULL.
   *
   * @param obj the variable to check.
   * @param name the name to use in the exception message.
   *
   * @return the variable.
   *
   * @throws IllegalArgumentException if the variable is NULL.
   */
  public static <T> T notNull(T obj, String name) {
    if (obj == null) {
      throw new IllegalArgumentException(name + " cannot be null");
    }
    return obj;
  }

}
