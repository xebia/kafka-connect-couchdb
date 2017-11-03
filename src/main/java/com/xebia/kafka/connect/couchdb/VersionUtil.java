package com.xebia.kafka.connect.couchdb;

class VersionUtil {
  public static String getVersion() {
    try {
      return VersionUtil.class.getPackage().getImplementationVersion();
    } catch(Exception ex){
      return "0.1.0";
    }
  }
}
