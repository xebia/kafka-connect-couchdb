package com.xebia.kafka.connect.couchdb;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;

import java.util.Map;


public class CouchDBSourceConnectorConfig extends AbstractConfig {

  public static final String COUCHDB_SETTING_CONFIG = "my.setting";
  private static final String COUCHDB_SETTING_DOC = "This is a setting important to my connector.";

  public CouchDBSourceConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
    super(config, parsedConfig);
  }

  public CouchDBSourceConnectorConfig(Map<String, String> parsedConfig) {
    this(conf(), parsedConfig);
  }

  public static ConfigDef conf() {
    return new ConfigDef()
        .define(COUCHDB_SETTING_CONFIG, Type.STRING, Importance.HIGH, COUCHDB_SETTING_DOC);
  }

  public String getMy(){
    return this.getString(COUCHDB_SETTING_CONFIG);
  }
}
