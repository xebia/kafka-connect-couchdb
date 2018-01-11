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

package com.xebia.kafka.connect.couchdb.converting;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.IOException;

public class JSONConverter implements Converter {
  private static JsonConverter jsonConverter = new JsonConverter();

  @Override
  public JsonObject fromRecord(SinkRecord record) {
    byte[] valueBytes = jsonConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
    try {
      return Json.mapper.readValue(valueBytes, JsonObject.class);
    } catch (IOException e) {
      throw new DataException("Could not parse record value bytes as JSON");
    }
  }

  @Override
  public SchemaAndValue toRecordSchemaAndValue(String topic, JsonObject doc) {
    return jsonConverter.toConnectData(topic, doc.encode().getBytes());
  }
}
