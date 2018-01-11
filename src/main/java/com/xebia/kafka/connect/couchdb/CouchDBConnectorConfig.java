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

import com.xebia.kafka.connect.couchdb.converting.Converter;
import com.xebia.kafka.connect.couchdb.merging.Merger;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.net.JksOptions;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class CouchDBConnectorConfig extends AbstractConfig {
  static class Constants {
    static final String AUTH_HEADER = "Authorization";
    static final int CONFLICT_STATUS_CODE = 409;
  }

  private static final String DATABASE_GROUP = "Database";
  private static final String CONNECTOR_GROUP = "Connector";

  private static final String COUCHDB_HOST_CONFIG = "host";
  private static final String COUCHDB_HOST_DISPLAY = "CouchDB host";
  private static final String COUCHDB_HOST_DOC = "The host name where CouchDB is reachable on.";

  private static final String COUCHDB_PORT_CONFIG = "port";
  private static final String COUCHDB_PORT_DISPLAY = "CouchDB port";
  private static final String COUCHDB_PORT_DOC = "The port where CouchDB is reachable on.";
  private static final int COUCHDB_PORT_DEFAULT = 5984;

  private static final String COUCHDB_USERNAME_CONFIG = "username";
  private static final String COUCHDB_USERNAME_DISPLAY = "CouchDB username";
  private static final String COUCHDB_USERNAME_DOC =
    "The username used for authenticating with CouchDB. Leave empty for public databases";
  private static final String COUCHDB_USERNAME_DEFAULT = "";

  private static final String COUCHDB_PASSWORD_CONFIG = "password";
  private static final String COUCHDB_PASSWORD_DISPLAY = "CouchDB password";
  private static final String COUCHDB_PASSWORD_DOC =
    "The password used for authenticating with CouchDB. Leave empty for public databases";
  private static final String COUCHDB_PASSWORD_DEFAULT = "";

  private static final String TOPICS_TO_DATABASES_MAPPING_CONFIG = "topics-to-databases-mapping";
  private static final String TOPICS_TO_DATABASES_MAPPING_DISPLAY = "Kafka topic -> CouchDB database mapping";
  private static final String TOPICS_TO_DATABASES_MAPPING_DOC =
    "A comma separated list of key/value pairs specifying which Kafka topic should be stored in which " +
      "CouchDB database. The database will need to be present in CouchDB. The key/value pairs should " +
      "follow the following syntax: {topic}/{database}";

  private static final String PARSER_CONFIG = "parser";
  private static final String PARSER_DISPLAY = "The parser class to use";
  private static final String PARSER_DOC =
    "A class implementing com.xebia.kafka.connect.couchdb.parsing.Parser. " +
      "This will be used to parse from a Kafka record to JSON and vice versa.";

  private static final String MERGER_CONFIG = "merger";
  private static final String MERGER_DISPLAY = "The merger class to use";
  private static final String MERGER_DOC =
    "A class implementing com.xebia.kafka.connect.couchdb.merging.Merger. " +
      "This will be used when conflicting documents have been detected to decide a winning revision.";

  static final String MAX_CONFLICTING_DOCS_FETCH_RETRIES_CONFIG =
    "max-conflicting-docs-fetch-retries";
  private static final String MAX_CONFLICTING_DOCS_FETCH_RETRIES_DISPLAY =
    "Maximum number of conflicting docs fetch retries";
  private static final String MAX_CONFLICTING_DOCS_FETCH_RETRIES_DOC =
    "How many times the connector should retry when the conflicts retrieval process fails. " +
      "A certain amount of retries can be expected when multiple processes are resolving conflicts in " +
      "parallel";

  static final String SOURCE_MAX_BATCH_SIZE_CONFIG = "max-source-batch-size";
  private static final String SOURCE_MAX_BATCH_SIZE_DISPLAY = "Maximum source batch size";
  private static final String SOURCE_MAX_BATCH_SIZE_DOC =
    "When the source connector is polled by Kafka the in-memory queue with CouchDB documents will be called " +
      "to fetch the new data. " +
      "When the queue contains multiple items they will be fetched until the size in this setting is reached " +
      "or the queue is depleted. " +
      "As such creating a batch mechanism to empty the queue.";

  private static ConfigDef baseConfigDef() {
    return new ConfigDef()
      .define(COUCHDB_HOST_CONFIG,
        ConfigDef.Type.STRING,
        ConfigDef.Importance.HIGH,
        COUCHDB_HOST_DOC,
        DATABASE_GROUP, 1,
        ConfigDef.Width.LONG,
        COUCHDB_HOST_DISPLAY)

      .define(COUCHDB_PORT_CONFIG,
        ConfigDef.Type.INT,
        COUCHDB_PORT_DEFAULT,
        ConfigDef.Importance.HIGH,
        COUCHDB_PORT_DOC,
        DATABASE_GROUP, 2,
        ConfigDef.Width.SHORT,
        COUCHDB_PORT_DISPLAY)

      .define(COUCHDB_USERNAME_CONFIG,
        ConfigDef.Type.STRING,
        COUCHDB_USERNAME_DEFAULT,
        ConfigDef.Importance.HIGH,
        COUCHDB_USERNAME_DOC,
        DATABASE_GROUP, 3,
        ConfigDef.Width.SHORT,
        COUCHDB_USERNAME_DISPLAY)

      .define(COUCHDB_PASSWORD_CONFIG,
        ConfigDef.Type.STRING,
        COUCHDB_PASSWORD_DEFAULT,
        ConfigDef.Importance.HIGH,
        COUCHDB_PASSWORD_DOC,
        DATABASE_GROUP, 4,
        ConfigDef.Width.SHORT,
        COUCHDB_PASSWORD_DISPLAY)

      .define(TOPICS_TO_DATABASES_MAPPING_CONFIG,
        ConfigDef.Type.STRING,
        ConfigDef.Importance.HIGH,
        TOPICS_TO_DATABASES_MAPPING_DOC,
        CONNECTOR_GROUP, 1,
        ConfigDef.Width.LONG,
        TOPICS_TO_DATABASES_MAPPING_DISPLAY)

      .define(PARSER_CONFIG,
        ConfigDef.Type.STRING,
        ConfigDef.Importance.HIGH,
        PARSER_DOC,
        CONNECTOR_GROUP, 2,
        ConfigDef.Width.LONG,
        PARSER_DISPLAY)

      .define(MERGER_CONFIG,
        ConfigDef.Type.STRING,
        ConfigDef.Importance.HIGH,
        MERGER_DOC,
        CONNECTOR_GROUP, 3,
        ConfigDef.Width.LONG,
        MERGER_DISPLAY)

      .define(MAX_CONFLICTING_DOCS_FETCH_RETRIES_CONFIG,
        ConfigDef.Type.INT,
        ConfigDef.Importance.MEDIUM,
        MAX_CONFLICTING_DOCS_FETCH_RETRIES_DOC,
        CONNECTOR_GROUP, 4,
        ConfigDef.Width.LONG,
        MAX_CONFLICTING_DOCS_FETCH_RETRIES_DISPLAY)

      .define(SOURCE_MAX_BATCH_SIZE_CONFIG,
        ConfigDef.Type.INT,
        ConfigDef.Importance.MEDIUM,
        SOURCE_MAX_BATCH_SIZE_DOC,
        CONNECTOR_GROUP, 5,
        ConfigDef.Width.LONG,
        SOURCE_MAX_BATCH_SIZE_DISPLAY);
  }

  static final ConfigDef config = baseConfigDef();

  private CouchDBConnectorConfig(ConfigDef config, Map<String, String> props) {
    super(config, props);
  }

  CouchDBConnectorConfig(Map<String, String> props) {
    this(config, props);
  }

  List<Map<String, String>> getTaskConfigs(int maxTasks) {
    List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);

    for (int i = 0; i < maxTasks; i++) {
      Map<String, String> configStringMap = new HashMap<>();
      for (String key : values().keySet()) {
        configStringMap.put(key, values().get(key).toString());
      }

      taskConfigs.add(configStringMap);
    }

    return taskConfigs;
  }

  private Map<String, String> getMapping(String configKey) {
    String mappingString = getString(configKey);
    String[] keyValuePairs = mappingString.split(",");

    Map<String, String> mapping = new HashMap<>();
    for (String keyValuePair : keyValuePairs) {
      String[] keyValue = keyValuePair.split("/");
      mapping.put(keyValue[0], keyValue[1]);
    }

    return mapping;
  }

  Map<String, String> getTopicsToDatabasesMapping() {
    return getMapping(TOPICS_TO_DATABASES_MAPPING_CONFIG);
  }

  HttpClientOptions getHttpClientOptions() {
    HttpClientOptions options = new HttpClientOptions();
    options.setDefaultHost(getString("host"));
    options.setDefaultPort(getInt("port"));

    if (getBoolean("ssl")) {
      options.setSsl(true);
      options.setTrustStoreOptions(new JksOptions()
        .setPath(getString("ssl-truststore-path"))
        .setPassword(getString("ssl-truststore-password"))
      );
    }

    return options;
  }

  String getBasicAuth() {
    String username = getString("username");
    String password = getString("password");

    String auth;
    if (username.isEmpty() || password.isEmpty()) {
      auth = "";
    } else {
      auth = username + ":" + password;
    }

    return "Basic " + Base64
      .getEncoder()
      .encodeToString(auth.getBytes());
  }

  Converter getConverter() {
    String converterClassName = getString("converter");
    try {
      Class<?> converterClass = getClass().getClassLoader().loadClass(converterClassName);
      Constructor<?> convertConst = converterClass.getConstructor();
      return (Converter) convertConst.newInstance();
    } catch (
      ClassNotFoundException |
        NoSuchMethodException |
        IllegalAccessException |
        InvocationTargetException |
        InstantiationException e) {
      throw new ConfigException(
        "Could not create an instance of converter, " +
          "does '" + converterClassName + "' exist and extend 'com.xebia.kafka.connect.couchdb.Converter'?",
        e
      );
    }
  }

  Merger getMerger() {
    String mergerClassName = getString("merger");
    try {
      Class<?> mergerClass = getClass().getClassLoader().loadClass(mergerClassName);
      Constructor<?> mergerConst = mergerClass.getConstructor();
      return (Merger) mergerConst.newInstance();
    } catch (
      ClassNotFoundException |
        NoSuchMethodException |
        IllegalAccessException |
        InvocationTargetException |
        InstantiationException e) {
      throw new ConfigException(
        "Could not create an instance of merger, " +
          "does '" + mergerClassName + "' exist and extend 'com.xebia.kafka.connect.couchdb.Merger'?",
        e
      );
    }
  }
}
