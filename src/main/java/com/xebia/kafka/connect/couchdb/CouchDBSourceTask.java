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
import io.vertx.rxjava.core.buffer.Buffer;
import io.vertx.rxjava.core.http.HttpClient;
import io.vertx.rxjava.core.http.HttpClientRequest;
import io.vertx.rxjava.core.http.HttpClientResponse;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Emitter;
import rx.Observable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.xebia.kafka.connect.couchdb.CouchDBConnectorConfig.Constants.AUTH_HEADER;
import static com.xebia.kafka.connect.couchdb.CouchDBConnectorConfig.SOURCE_MAX_BATCH_SIZE_CONFIG;

public class CouchDBSourceTask extends SourceTask {
  static class Acc {
    String str;
    JsonObject obj;

    Acc(String str, JsonObject obj) {
      this.str = str;
      this.obj = obj;
    }

    Acc(String str) {
      this.str = str;
    }

    Acc() {
      this.str = "";
    }

    boolean hasObject() {
      return Objects.nonNull(obj);
    }

    JsonObject getObj() {
      return obj;
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(CouchDBSourceTask.class);

  private String auth;
  private Map<String, String> databasesMapping;
  private Map<String, String> changesSinceMapping;
  private Converter converter;
  private int maxBatchSize;

  private HttpClient httpClient;
  private BlockingQueue<SourceRecord> records;

  SourceRecord createRecord(String dbName, String seq, String topic, JsonObject doc, Converter converter) {
    Map<String, String> sourcePartition = new HashMap<>();
    sourcePartition.put("database", dbName);

    Map<String, String> sourceOffset = new HashMap<>();
    sourceOffset.put("seq", seq);

    Schema keySchema = Schema.STRING_SCHEMA;
    String key = doc.getString("_id");

    byte[] docBytes = doc.encode().getBytes();
    SchemaAndValue schemaAndValue = converter.toConnectData(topic, docBytes);

    return new SourceRecord(
      sourcePartition,
      sourceOffset,
      topic,
      keySchema,
      key,
      schemaAndValue.schema(),
      schemaAndValue.value()
    );
  }

  Acc accumulateJsonObjects(Acc acc, String chunk) {
    String concat = acc.str + chunk;

    if (concat.contains("\n")) {
      String[] parts = concat.split("\n");
      JsonObject jObj = Json.mapper.convertValue(parts[0], JsonObject.class);

      if (parts.length > 1) {
        return new Acc(parts[1], jObj);
      }
      return new Acc("", jObj);
    }

    return new Acc(concat);
  }

  private Observable<HttpClientResponse> get(String requestURI) {
    return Observable.create(subscriber -> {
      HttpClientRequest req = httpClient
        .get(requestURI)
        .putHeader(AUTH_HEADER, auth);

      Observable<HttpClientResponse> resp = req.toObservable();
      resp.subscribe(subscriber);

      req.end();

    }, Emitter.BackpressureMode.BUFFER);
  }

  boolean isNotDesignDocument(JsonObject doc) {
    return !doc.getString("id").startsWith("_design");
  }

  private void initChangesFeeds() {
    Observable
      .from(databasesMapping.entrySet())
      .flatMap(entry -> {
        String topic = entry.getKey();
        String dbName = entry.getValue();

        String changesSince = "0";
        try {
          changesSince = changesSinceMapping.get(dbName);
        } catch (NullPointerException ignore) {
          LOG.warn(
            "No 'changes since' configuration found for database '" + dbName + "'. " +
              "Using default: " + changesSince
          );
        }

        return get("/" + dbName + "/_changes?feed=continuous&include_docs=true&since=" + changesSince)
          .retry()
          .flatMap(HttpClientResponse::toObservable)
          .map(Buffer::toString)
          .scan(new Acc(), this::accumulateJsonObjects)
          .filter(Acc::hasObject)
          .map(Acc::getObj)
          .filter(this::isNotDesignDocument)
          .map(change -> {
            String seq = change.getString("seq");
            JsonObject doc = change.getJsonObject("doc");
            return records.offer(createRecord(dbName, seq, topic, doc, converter));
          })
          .doOnError(e -> LOG.error("Error while listening to changes from '" + dbName + "' database", e));
      })
      .toCompletable()
      .await();
  }

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> properties) {
    CouchDBConnectorConfig config = new CouchDBConnectorConfig(properties);
    auth = config.getBasicAuth();
    databasesMapping = config.getSourceTopicsToDatabasesMapping();
    converter = config.getConverter();
    converter.configure(Collections.singletonMap("schemas.enable", false), false);
    maxBatchSize = config.getInt(SOURCE_MAX_BATCH_SIZE_CONFIG);
    changesSinceMapping = config.getDatabasesToChangesSinceMapping();

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
    while (records.peek() != null && batch.size() < maxBatchSize) {
      batch.add(records.take());
    }

    return batch;
  }

  @Override
  public void stop() {
    httpClient.close();
  }
}
