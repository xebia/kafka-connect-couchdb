package com.xebia.kafka.connect.couchdb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CouchDBSinkConnector extends SinkConnector {
  private static Logger log = LoggerFactory.getLogger(CouchDBSinkConnector.class);
  private CouchDBSinkConnectorConfig config;

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> map) {
    config = new CouchDBSinkConnectorConfig(map);
  }

  @Override
  public Class<? extends Task> taskClass() {
    return CouchDBSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);

    for (int i = 0; i < maxTasks; i++) {
      Map<String, String> configStringMap = new HashMap<>();
      for (String key : config.values().keySet()) {
        configStringMap.put(key, config.values().get(key).toString());
      }

      taskConfigs.add(configStringMap);
    }

    return taskConfigs;
  }

  @Override
  public void stop() {
  }

  @Override
  public ConfigDef config() {
    return CouchDBSinkConnectorConfig.config;
  }
}
