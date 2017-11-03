package com.xebia.kafka.connect.couchdb;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class CouchDBSinkConnectorConfig extends AbstractConfig {
  public static final String DATABASE_GROUP = "Database";
  public static final String CONNECTOR_GROUP = "Connector";

  public static final String COUCHDB_HOST_CONFIG = "host";
  static final String COUCHDB_HOST_DISPLAY = "CouchDB host";
  static final String COUCHDB_HOST_DOC = "The host name where CouchDB is reachable on.";

  public static final String COUCHDB_PORT_CONFIG = "port";
  static final String COUCHDB_PORT_DISPLAY = "CouchDB port";
  static final String COUCHDB_PORT_DOC = "The port where CouchDB is reachable on.";
  public static final int COUCHDB_PORT_DEFAULT = 5984;

  public static final String COUCHDB_USERNAME_CONFIG = "username";
  static final String COUCHDB_USERNAME_DISPLAY = "CouchDB username";
  static final String COUCHDB_USERNAME_DOC =
    "The username used for authenticating with CouchDB. Leave empty for public databases";
  public static final String COUCHDB_USERNAME_DEFAULT = "";

  public static final String COUCHDB_PASSWORD_CONFIG = "password";
  static final String COUCHDB_PASSWORD_DISPLAY = "CouchDB password";
  static final String COUCHDB_PASSWORD_DOC =
    "The password used for authenticating with CouchDB. Leave empty for public databases";
  public static final String COUCHDB_PASSWORD_DEFAULT = "";

  public static final String COUCHDB_TOPICS_TO_DATABASES_MAPPING_CONFIG = "topics-to-databases-mapping";
  static final String COUCHDB_TOPICS_TO_DATABASES_MAPPING_DISPLAY = "Kafka topic -> CouchDB database mapping";
  static final String COUCHDB_TOPICS_TO_DATABASES_MAPPING_DOC =
    "A comma separated list of key/value pairs specifying which Kafka topic should be stored in which " +
      "CouchDB database. The database will need to be present in CouchDB. The key/value pairs should " +
      "follow the following syntax: {topic}/{database}";

  public static final String COUCHDB_TOPICS_TO_ID_FIELDS_MAPPING_CONFIG = "topics-to-id-fields-mapping";
  static final String COUCHDB_TOPICS_TO_ID_FIELDS_MAPPING_DISPLAY = "Kafka topic -> id field mapping";
  static final String COUCHDB_TOPICS_TO_ID_FIELDS_MAPPING_DOC =
    "A comma separated list of key/value pairs specifying which field in an object from a specific Kafka " +
      "topic should be used to check if a previous version of that object is already present in the " +
      "database. This will decide if a new document will be created or an existing one will be updated. An " +
      "index needs to be present for the given field in CouchDB. The key/value pairs should follow the " +
      "following syntax: {topic}/{field}";

  static ConfigDef config = baseConfigDef();

  public CouchDBSinkConnectorConfig(ConfigDef config, Map<String, String> props) {
    super(config, props);
  }

  public CouchDBSinkConnectorConfig(Map<String, String> props) {
    this(config, props);
  }

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
        DATABASE_GROUP, 1,
        ConfigDef.Width.SHORT,
        COUCHDB_PORT_DISPLAY)

      .define(COUCHDB_USERNAME_CONFIG,
        ConfigDef.Type.STRING,
        COUCHDB_USERNAME_DEFAULT,
        ConfigDef.Importance.HIGH,
        COUCHDB_USERNAME_DOC,
        DATABASE_GROUP, 1,
        ConfigDef.Width.SHORT,
        COUCHDB_USERNAME_DISPLAY)

      .define(COUCHDB_PASSWORD_CONFIG,
        ConfigDef.Type.STRING,
        COUCHDB_PASSWORD_DEFAULT,
        ConfigDef.Importance.HIGH,
        COUCHDB_PASSWORD_DOC,
        DATABASE_GROUP, 1,
        ConfigDef.Width.SHORT,
        COUCHDB_PASSWORD_DISPLAY)

      .define(COUCHDB_TOPICS_TO_DATABASES_MAPPING_CONFIG,
        ConfigDef.Type.STRING,
        ConfigDef.Importance.HIGH,
        COUCHDB_TOPICS_TO_DATABASES_MAPPING_DOC,
        DATABASE_GROUP, 1,
        ConfigDef.Width.LONG,
        COUCHDB_TOPICS_TO_DATABASES_MAPPING_DISPLAY)

      .define(COUCHDB_TOPICS_TO_ID_FIELDS_MAPPING_CONFIG,
        ConfigDef.Type.STRING,
        ConfigDef.Importance.HIGH,
        COUCHDB_TOPICS_TO_ID_FIELDS_MAPPING_DOC,
        DATABASE_GROUP, 1,
        ConfigDef.Width.LONG,
        COUCHDB_TOPICS_TO_ID_FIELDS_MAPPING_DISPLAY)

      ;
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

  Map<String, String> getKafkaTopicsToCouchDBDatabasesMapping() {
    return getMapping(COUCHDB_TOPICS_TO_DATABASES_MAPPING_CONFIG);
  }

  Map<String, String> getKafkaTopicsToIdFieldsMapping() {
    return getMapping(COUCHDB_TOPICS_TO_ID_FIELDS_MAPPING_CONFIG);
  }

  String getAuth() {
    String username = getString("username");
    String password = getString("password");

    String auth;
    if (username.isEmpty() || password.isEmpty()) {
      auth = "";
    } else {
      auth = username + ":" + password;
    }

    return Base64
      .getEncoder()
      .encodeToString(auth.getBytes());
  }
}
