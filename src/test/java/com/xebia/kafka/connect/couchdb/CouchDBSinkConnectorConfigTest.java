package com.xebia.kafka.connect.couchdb;

import org.junit.Test;

public class CouchDBSinkConnectorConfigTest {
  @Test
  public void doc() {
    System.out.println(CouchDBSinkConnectorConfig.config.toRst());
  }
}
