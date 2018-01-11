/*
 * Copyright 2018 Xebia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.xebia.kafka.connect.couchdb;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.List;
import java.util.Map;

public class CouchDBSourceConnector extends SourceConnector {
  private CouchDBConnectorConfig config;

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> map) {
    config = new CouchDBConnectorConfig(map);
  }

  @Override
  public Class<? extends Task> taskClass() {
    return CouchDBSourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    return config.getTaskConfigs(maxTasks);
  }

  @Override
  public void stop() {
  }

  @Override
  public ConfigDef config() {
    return CouchDBConnectorConfig.config;
  }
}
