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

import com.xebia.kafka.connect.couchdb.merging.Merger;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.net.JksOptions;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
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

  private static final String COUCHDB_SSL_CONFIG = "ssl";
  private static final String COUCHDB_SSL_DISPLAY = "Connect to CouchDB over SSL";
  private static final String COUCHDB_SSL_DOC =
    "Whether or not to connect to CouchDB over SSL. " +
      "If your CouchDB instance is connected to an open network this should be set to true!" +
      "If true, you'll need to provide a SSL trust store using the 'ssl-truststore-path' " +
      "and 'ssl-truststore-password' configuration items.";
  private static final boolean COUCHDB_SSL_DEFAULT = false;

  private static final String SSL_TRUSTSTORE_PATH_CONFIG = "ssl-truststore-path";
  private static final String SSL_TRUSTSTORE_PATH_DISPLAY = "Path to the trust store file to use.";
  private static final String SSL_TRUSTSTORE_PATH_DOC =
    "The trust store should be a file reachable by the application. It should be loaded with the SSL " +
      "certificate in use by CouchDB to encrypt traffic over HTTPS connections.";
  private static final String SSL_TRUSTSTORE_PATH_DEFAULT = "";

  private static final String SSL_TRUSTSTORE_PASSWORD_CONFIG = "ssl-truststore-password";
  private static final String SSL_TRUSTSTORE_PASSWORD_DISPLAY = "Password for the trust store file to use";
  private static final String SSL_TRUSTSTORE_PASSWORD_DOC =
    "If the trust store provided in 'ssl-truststore-path' is password protected, " +
      "you'll need to provide that password here.";
  private static final String SSL_TRUSTSTORE_PASSWORD_DEFAULT = "";

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

  private static final String TOPICS_CONFIG = "topics";
  private static final String TOPICS_DISPLAY = "The Kafka topics to use";
  private static final String TOPICS_DOC =
    "A comma separated list of Kafka topics that are relevant to this connector.";

  private static final String SINK_TOPICS_TO_DATABASES_MAPPING_CONFIG =
    "sink-topics-to-databases-mapping";
  private static final String SINK_TOPICS_TO_DATABASES_MAPPING_DISPLAY = 
    "Kafka topic -> CouchDB database mapping for sink";
  private static final String SINK_TOPICS_TO_DATABASES_MAPPING_DOC =
    "A comma separated list of key/value pairs specifying which Kafka topic should be stored in which " +
      "CouchDB database. The database will need to be present in CouchDB. " +
      "The key/value pairs should follow the following syntax: {topic}/{database}";

  private static final String SOURCE_TOPICS_TO_DATABASES_MAPPING_CONFIG =
    "source-topics-to-databases-mapping";
  private static final String SOURCE_TOPICS_TO_DATABASES_MAPPING_DISPLAY =
    "Kafka topic -> CouchDB database mapping for source";
  private static final String SOURCE_TOPICS_TO_DATABASES_MAPPING_DOC =
    "A comma separated list of key/value pairs specifying which CouchDB database should be listened to " +
      "for their changes and to which Kafka topic those changes should be published. " +
      "The key/value pairs should follow the following syntax: {topic}/{database}";

  private static final String TOPICS_TO_ID_FIELDS_MAPPING_CONFIG = "topics-to-id-fields-mapping";
  private static final String TOPICS_TO_ID_FIELDS_MAPPING_DISPLAY = "Kafka topic -> JSON id field name";
  private static final String TOPICS_TO_ID_FIELDS_MAPPING_DOC =
    "A comma separated list of key/value pairs specifying which field from a Kafka record's value should " +
      "be used as the ID. The ID is used to identify a unique document within CouchDB." +
      "The key/value pairs should follow the following syntax: {topic}/{database}";

  private static final String DATABASES_TO_CHANGES_SINCE_MAPPING_CONFIG =
    "databases-to-changes-since-mapping";
  private static final String DATABASES_TO_CHANGES_SINCE_MAPPING_DISPLAY =
    "CouchDB database -> changes since setting";
  private static final String DATABASES_TO_CHANGES_SINCE_MAPPING_DOC =
    "A comma separated list of key/value pairs specifying which changes should be published per database. " +
      "'0' specifies all changes since the beginning of time. " +
      "'now' specifies all changes after the connector starts listening. " +
      "A specific update sequence ID specifies changes since that update.";

  private static final String CONVERTER_CONFIG = "converter";
  private static final String CONVERTER_DISPLAY = "The converter class to use";
  private static final String CONVERTER_DOC =
    "A class implementing com.xebia.kafka.connect.couchdb.parsing.Converter. " +
      "This will be used to parse from a Kafka record to JSON and vice versa.";
  private static final String CONVERTER_DEFAULT = "com.xebia.kafka.connect.couchdb.converting.JSONConverter";

  private static final String MERGER_CONFIG = "merger";
  private static final String MERGER_DISPLAY = "The merger class to use";
  private static final String MERGER_DOC =
    "A class implementing com.xebia.kafka.connect.couchdb.merging.Merger. " +
      "This will be used when conflicting documents have been detected to decide a winning revision.";
  private static final String MERGER_DEFAULT = "com.xebia.kafka.connect.couchdb.merging.LatestWinsMerger";

  static final String MAX_CONFLICTING_DOCS_FETCH_RETRIES_CONFIG =
    "max-conflicting-docs-fetch-retries";
  private static final String MAX_CONFLICTING_DOCS_FETCH_RETRIES_DISPLAY =
    "Maximum number of conflicting docs fetch retries";
  private static final String MAX_CONFLICTING_DOCS_FETCH_RETRIES_DOC =
    "How many times the connector should retry when the conflicts retrieval process fails. " +
      "A certain amount of retries can be expected when multiple processes are resolving conflicts in " +
      "parallel";
  private static final int MAX_CONFLICTING_DOCS_FETCH_RETRIES_DEFAULT = 5;

  static final String SOURCE_MAX_BATCH_SIZE_CONFIG = "max-source-batch-size";
  private static final String SOURCE_MAX_BATCH_SIZE_DISPLAY = "Maximum source batch size";
  private static final String SOURCE_MAX_BATCH_SIZE_DOC =
    "When the source connector is polled by Kafka the in-memory queue with CouchDB documents will be " +
      "called to fetch the new data. " +
      "When the queue contains multiple items they will be fetched until the size in this setting is " +
      "reached or the queue is depleted. " +
      "As such creating a batch mechanism to empty the queue.";
  private static final int SOURCE_MAX_BATCH_SIZE_DEFAULT = 12;

  private static final Logger LOG = LoggerFactory.getLogger(CouchDBConnectorConfig.class);

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

      .define(COUCHDB_SSL_CONFIG,
        ConfigDef.Type.BOOLEAN,
        COUCHDB_SSL_DEFAULT,
        ConfigDef.Importance.HIGH,
        COUCHDB_SSL_DOC,
        DATABASE_GROUP, 3,
        ConfigDef.Width.SHORT,
        COUCHDB_SSL_DISPLAY)

      .define(SSL_TRUSTSTORE_PATH_CONFIG,
        ConfigDef.Type.STRING,
        SSL_TRUSTSTORE_PATH_DEFAULT,
        ConfigDef.Importance.HIGH,
        SSL_TRUSTSTORE_PATH_DOC,
        DATABASE_GROUP, 4,
        ConfigDef.Width.SHORT,
        SSL_TRUSTSTORE_PATH_DISPLAY)

      .define(SSL_TRUSTSTORE_PASSWORD_CONFIG,
        ConfigDef.Type.STRING,
        SSL_TRUSTSTORE_PASSWORD_DEFAULT,
        ConfigDef.Importance.HIGH,
        SSL_TRUSTSTORE_PASSWORD_DOC,
        DATABASE_GROUP, 5,
        ConfigDef.Width.SHORT,
        SSL_TRUSTSTORE_PASSWORD_DISPLAY)

      .define(COUCHDB_USERNAME_CONFIG,
        ConfigDef.Type.STRING,
        COUCHDB_USERNAME_DEFAULT,
        ConfigDef.Importance.HIGH,
        COUCHDB_USERNAME_DOC,
        DATABASE_GROUP, 6,
        ConfigDef.Width.SHORT,
        COUCHDB_USERNAME_DISPLAY)

      .define(COUCHDB_PASSWORD_CONFIG,
        ConfigDef.Type.STRING,
        COUCHDB_PASSWORD_DEFAULT,
        ConfigDef.Importance.HIGH,
        COUCHDB_PASSWORD_DOC,
        DATABASE_GROUP, 7,
        ConfigDef.Width.SHORT,
        COUCHDB_PASSWORD_DISPLAY)

      .define(TOPICS_CONFIG,
        ConfigDef.Type.STRING,
        ConfigDef.Importance.HIGH,
        TOPICS_DOC,
        CONNECTOR_GROUP, 1,
        ConfigDef.Width.LONG,
        TOPICS_DISPLAY)

      .define(SINK_TOPICS_TO_DATABASES_MAPPING_CONFIG,
        ConfigDef.Type.STRING,
        ConfigDef.Importance.HIGH,
        SINK_TOPICS_TO_DATABASES_MAPPING_DOC,
        CONNECTOR_GROUP, 2,
        ConfigDef.Width.LONG,
        SINK_TOPICS_TO_DATABASES_MAPPING_DISPLAY)

      .define(SOURCE_TOPICS_TO_DATABASES_MAPPING_CONFIG,
        ConfigDef.Type.STRING,
        ConfigDef.Importance.HIGH,
        SOURCE_TOPICS_TO_DATABASES_MAPPING_DOC,
        CONNECTOR_GROUP, 3,
        ConfigDef.Width.LONG,
        SOURCE_TOPICS_TO_DATABASES_MAPPING_DISPLAY)

      .define(TOPICS_TO_ID_FIELDS_MAPPING_CONFIG,
        ConfigDef.Type.STRING,
        ConfigDef.Importance.HIGH,
        TOPICS_TO_ID_FIELDS_MAPPING_DOC,
        CONNECTOR_GROUP, 4,
        ConfigDef.Width.LONG,
        TOPICS_TO_ID_FIELDS_MAPPING_DISPLAY)

      .define(DATABASES_TO_CHANGES_SINCE_MAPPING_CONFIG,
        ConfigDef.Type.STRING,
        ConfigDef.Importance.HIGH,
        DATABASES_TO_CHANGES_SINCE_MAPPING_DOC,
        CONNECTOR_GROUP, 5,
        ConfigDef.Width.LONG,
        DATABASES_TO_CHANGES_SINCE_MAPPING_DISPLAY)

      .define(CONVERTER_CONFIG,
        ConfigDef.Type.STRING,
        CONVERTER_DEFAULT,
        ConfigDef.Importance.MEDIUM,
        CONVERTER_DOC,
        CONNECTOR_GROUP, 6,
        ConfigDef.Width.LONG,
        CONVERTER_DISPLAY)

      .define(MERGER_CONFIG,
        ConfigDef.Type.STRING,
        MERGER_DEFAULT,
        ConfigDef.Importance.MEDIUM,
        MERGER_DOC,
        CONNECTOR_GROUP, 7,
        ConfigDef.Width.LONG,
        MERGER_DISPLAY)

      .define(MAX_CONFLICTING_DOCS_FETCH_RETRIES_CONFIG,
        ConfigDef.Type.INT,
        MAX_CONFLICTING_DOCS_FETCH_RETRIES_DEFAULT,
        ConfigDef.Importance.MEDIUM,
        MAX_CONFLICTING_DOCS_FETCH_RETRIES_DOC,
        CONNECTOR_GROUP, 8,
        ConfigDef.Width.LONG,
        MAX_CONFLICTING_DOCS_FETCH_RETRIES_DISPLAY)

      .define(SOURCE_MAX_BATCH_SIZE_CONFIG,
        ConfigDef.Type.INT,
        SOURCE_MAX_BATCH_SIZE_DEFAULT,
        ConfigDef.Importance.MEDIUM,
        SOURCE_MAX_BATCH_SIZE_DOC,
        CONNECTOR_GROUP, 9,
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

  List<String> getListing(String listingString) {
    return Arrays.asList(listingString.split(","));
  }

  List<String> getTopics() {
    return getListing(getString(TOPICS_CONFIG));
  }

  Map<String, String> getMapping(String mappingString) {
    String[] keyValuePairs = mappingString.split(",");

    Map<String, String> mapping = new HashMap<>();
    for (String keyValuePair : keyValuePairs) {
      String[] keyValue = keyValuePair.split("/");
      if (keyValue.length == 2) {
        mapping.put(keyValue[0], keyValue[1]);
      }
    }

    return mapping;
  }

  Map<String, String> getSinkTopicsToDatabasesMapping() {
    Map<String, String> sinkTopicsToDatabasesMapping =
      getMapping(getString(SINK_TOPICS_TO_DATABASES_MAPPING_CONFIG));

    for (String topic : getTopics()) {
      if (!sinkTopicsToDatabasesMapping.keySet().contains(topic)) {
        throw new ConfigException(
          "The Kafka topic '" + topic +
            "' does not have a corresponding '" + SINK_TOPICS_TO_DATABASES_MAPPING_CONFIG + "' setting"
        );
      }
    }

    for (String topic : sinkTopicsToDatabasesMapping.keySet()) {
      if (!getTopics().contains(topic)) {
        LOG.warn(
          "Topic '" + topic + "' was specified in " + SINK_TOPICS_TO_DATABASES_MAPPING_CONFIG +
            "but not in " + TOPICS_CONFIG + ". Kafka will not send this connector messages from that " +
            "topic until you also specify it in " + TOPICS_CONFIG
        );
      }
    }

    return sinkTopicsToDatabasesMapping;
  }

  Map<String, String> getSourceTopicsToDatabasesMapping() {
    Map<String, String> sourceTopicsToDatabasesMapping =
      getMapping(getString(SOURCE_TOPICS_TO_DATABASES_MAPPING_CONFIG));

    for (String topic : getTopics()) {
      if (!sourceTopicsToDatabasesMapping.keySet().contains(topic)) {
        throw new ConfigException(
          "The Kafka topic '" + topic +
            "' does not have a corresponding '" + SOURCE_TOPICS_TO_DATABASES_MAPPING_CONFIG + "' setting"
        );
      }
    }

    return sourceTopicsToDatabasesMapping;
  }

  Map<String, String> getTopicsToIdFieldsMapping() {
    Map<String, String> topicsToIdFieldsMapping = getMapping(getString(TOPICS_TO_ID_FIELDS_MAPPING_CONFIG));

    for (String sinkTopic : getSinkTopicsToDatabasesMapping().keySet()) {
      if (!topicsToIdFieldsMapping.keySet().contains(sinkTopic)) {
        throw new ConfigException(
          "The Kafka topic '" + sinkTopic +
            "' does not have a corresponding '" + TOPICS_TO_ID_FIELDS_MAPPING_CONFIG + "' setting"
        );
      }
    }

    return topicsToIdFieldsMapping;
  }

  Map<String, String> getDatabasesToChangesSinceMapping() {
    return getMapping(getString(DATABASES_TO_CHANGES_SINCE_MAPPING_CONFIG));
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

  @SuppressWarnings("unchecked")
  private <T> T getInstance(String className, Class<T> clazz) {
    try {
      Class<?> loadedClass = getClass().getClassLoader().loadClass(className);
      Constructor<?> classConstructor = loadedClass.getConstructor();
      return (T) classConstructor.newInstance();
    } catch (
      ClassNotFoundException |
        NoSuchMethodException |
        IllegalAccessException |
        InvocationTargetException |
        InstantiationException e) {
      throw new ConfigException(
        "Could not create an instance of " + clazz.getSimpleName() + ", " +
          "does '" + className + "' exist and extend '" + clazz.getCanonicalName() + "'?",
        e
      );
    }
  }

  Converter getConverter() {
    return getInstance(getString("converter"), Converter.class);
  }

  Merger getMerger() {
    return getInstance(getString("merger"), Merger.class);
  }
}
