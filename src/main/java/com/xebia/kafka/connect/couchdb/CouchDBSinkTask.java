package com.xebia.kafka.connect.couchdb;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.ext.web.client.WebClient;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Single;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

public class CouchDBSinkTask extends SinkTask {
  private static Logger log = LoggerFactory.getLogger(CouchDBSinkTask.class);

  private String host;
  private int port;
  private String auth;
  private Map<String, String> idFieldsMapping;
  private Map<String, String> databasesMapping;

  private WebClient webClient;

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> properties) {
    CouchDBSinkConnectorConfig config;
    try {
      config = new CouchDBSinkConnectorConfig(properties);
      auth = config.getAuth();
      idFieldsMapping = config.getKafkaTopicsToIdFieldsMapping();
      databasesMapping = config.getKafkaTopicsToCouchDBDatabasesMapping();
    } catch (ConfigException e) {
      throw new ConnectException("Couldn't start CouchDBSinkTask due to configuration error", e);
    }

    host = config.getString("host");
    port = config.getInt("port");

    WebClientOptions options = new WebClientOptions();
    webClient = WebClient.create(Vertx.vertx(), options);
  }

  private Single<Optional<JsonObject>> findById(String database, String idField, String id) {
    String query = "{\"selector\":{\"" + idField + "\":{\"$eq\":\"" + id + "\"}}}";
    return webClient
      .post(port, host, "/" + database)
      .putHeader("Authorization", "Basic " + auth)
      .rxSendJson(query)
      .map(response -> {
        Optional<JsonObject> result = Optional.empty();
        if (response.statusCode() == 200) {
          result = Optional.ofNullable(response.bodyAsJsonObject());
        }
        return result;
      });
  }

  private Single<Boolean> create(String database, JsonObject newObj) {
    return webClient
      .post(port, host, "/" + database)
      .rxSendJsonObject(newObj)
      .map(response -> response.statusCode() == 200);
  }

  private Single<Boolean> update(
    String database,
    String id,
    JsonObject oldObj,
    JsonObject newObj
  ) {
    JsonObject updatedObj = oldObj.mergeIn(newObj);
    return webClient
      .put(port, host, "/" + database + "/" + id)
      .rxSendJsonObject(updatedObj)
      .map(response -> response.statusCode() == 200);
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    JsonConverter jsonConverter = new JsonConverter();
    Observable<Boolean> failureResult = Observable.from(new Boolean[]{false});

    Observable
      .from(records)
      .map(record -> {
        String topic = record.topic();

        byte[] valueAsJsonBytes;
        try {
          valueAsJsonBytes = jsonConverter.fromConnectData(
            record.topic(),
            record.valueSchema(),
            record.value()
          );
        } catch (Exception e) {
          log.error("Could not parse Kafka message as JSON bytes {}", record.value(), e);
          return failureResult;
        }

        JsonObject newObj;
        try {
          newObj = Json.mapper.readValue(valueAsJsonBytes, JsonObject.class);
        } catch (IOException e) {
          log.error("Could not parse Kafka JSON as JsonObject {}", record.value(), e);
          return failureResult;
        }

        String database;
        try {
          database = databasesMapping.get(topic);
        } catch (NullPointerException e) {
          log.error("Not database mapping found for topic {}", topic);
          return failureResult;
        }

        String idField;
        try {
          idField = idFieldsMapping.get(topic);
        } catch (NullPointerException e) {
          log.error("Not id field mapping found for topic {}", topic);
          return failureResult;
        }

        String id = newObj.getString(idField);
        if (id == null) {
          log.error(
            "No value found for id field {} in topic {} message: \n{}",
            idField,
            topic,
            newObj.encodePrettily()
          );
          return failureResult;
        }

        return findById(database, idField, id)
          .flatMap(objOpt -> objOpt
            .map(oldObj -> update(database, id, oldObj, newObj))
            .orElseGet(() -> create(database, newObj))
          )
          .toObservable();
      })
      .toBlocking();
  }


  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
  }

  @Override
  public void stop() {
    webClient.close();
  }
}
