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

import com.fasterxml.jackson.core.JsonProcessingException;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.http.HttpClient;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.xebia.kafka.connect.couchdb.CouchDBConnectorConfig.Constants.*;
import static com.xebia.kafka.connect.couchdb.CouchDBConnectorConfig.SOURCE_MAX_BATCH_SIZE_CONFIG;

public class CouchDBSourceTask extends SourceTask {
  private static final Logger LOG = LoggerFactory.getLogger(CouchDBSourceTask.class);

  private String auth;
  private Map<String, String> databasesMapping;
  private Converter converter;
  private int maxBatchSize;

  private HttpClient httpClient;
  private BlockingQueue<SourceRecord> records;

  private SourceRecord createRecord(String dbName, String seq, String topic, JsonObject doc) {
    Map<String, String> partition = new HashMap<>();
    partition.put("database", dbName);

    Map<String, String> offset = new HashMap<>();
    partition.put("seq", seq);

    Schema keySchema = Schema.STRING_SCHEMA;
    String key = doc.getString("_id");

    byte[] docBytes;
    try {
      docBytes = Json.mapper.writeValueAsBytes(doc);
    } catch (JsonProcessingException e) {
      LOG.error("Could not get bytes from JSON value {} from database {}", doc.encodePrettily(), dbName);
      throw new RuntimeException("Could not get bytes from JSON value");
    }

    SchemaAndValue schemaAndValue = converter.toConnectData(topic, docBytes);

    return new SourceRecord(
      partition,
      offset,
      topic,
      keySchema,
      key,
      schemaAndValue.schema(),
      schemaAndValue.value()
    );
  }

  private void initChangesFeeds() {
    for (Map.Entry<String, String> entry : databasesMapping.entrySet()) {
      String dbName = entry.getKey();
      String topic = entry.getValue();

      httpClient
        .get("/" + dbName + "/_changes?feed=continuous&include_docs=true&since=now")
        .putHeader(AUTH_HEADER, auth)
        .handler(response -> response
          .handler(event -> {
            JsonObject change = event.toJsonObject();
            String id = change.getString("id");

            // We ignore design documents as they are not the kind of data we want to publish to Kafka
            if (!id.startsWith("_design")) {
              String seq = change.getString("id");
              JsonObject doc = change.getJsonObject("latestRev");

              records.offer(createRecord(dbName, seq, topic, doc));
            }
          })
        )
        .exceptionHandler(e -> LOG.error("Error while listening to changes for database {}", dbName, e))
        .end();
    }
  }

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> properties) {
    CouchDBConnectorConfig config = new CouchDBConnectorConfig(properties);
    auth = config.getBasicAuth();
    databasesMapping = config.getTopicsToDatabasesMapping();
    converter = config.getConverter();
    maxBatchSize = config.getInt(SOURCE_MAX_BATCH_SIZE_CONFIG);

    httpClient = Vertx.vertx().createHttpClient(config.getHttpClientOptions());

    records = new LinkedBlockingQueue<>();

    initChangesFeeds();
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    List<SourceRecord> batch = new ArrayList<>();

    // Block until a record becomes available
    records.peek();

    // While records are available add them to the list until the queue is depleted or we reach maxBatchSize
    while (records.peek() != null && batch.size() <= maxBatchSize) {
      batch.add(records.take());
    }

    return batch;
  }

  @Override
  public void stop() {
    httpClient.close();
  }
}
