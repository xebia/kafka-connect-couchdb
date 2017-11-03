package com.xebia.kafka.connect.couchdb;

import org.junit.Test;

public class CouchDBSourceConnectorConfigTest {
  @Test
  public void doc() {
    System.out.println(CouchDBSourceConnectorConfig.conf().toRst());
  }
}
