package org.apache.hadoop.security;

class RpcPasswordAndBypass {
  final private String rpcPassword;
  final private boolean bypass;

  RpcPasswordAndBypass(String rpcPassword, boolean bypass) {
    this.rpcPassword = rpcPassword;
    this.bypass = bypass;
  }
  public String getRpcPassword() {
    return rpcPassword;
  }
  public boolean isBypass() {
    return bypass;
  }
  public boolean isEmpty() {
    return rpcPassword == null;
  }
}
