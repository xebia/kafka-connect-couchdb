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

import io.vertx.core.json.JsonObject;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.Converter;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static com.xebia.kafka.connect.couchdb.TestUtils.TEST_LATEST_REV;
import static org.junit.jupiter.api.Assertions.*;

public class CouchDBSourceTaskTest {
  @Test
  public void createRecordTest() {
    CouchDBSourceTask sourceTask = new CouchDBSourceTask();
    Converter converter = new JsonConverter();
    converter.configure(Collections.singletonMap("schemas.enable", false), false);

    SourceRecord record = sourceTask.createRecord(
      "MyDatabase",
      "MySeq",
      "MyTopic",
      TEST_LATEST_REV,
      converter
    );

    assertEquals(
      "MyDatabase", record.sourcePartition().get("database"),
      "should use given database name as database in source partition"
    );
    assertEquals(
      "MySeq", record.sourceOffset().get("seq"),
      "should use given sequence as seq in source offset"
    );
    assertEquals(
      "MyTopic", record.topic(),
      "should use given topic as topic"
    );
    assertEquals(
      Schema.STRING_SCHEMA, record.keySchema(),
      "should use String schema as key schema"
    );
    assertEquals(
      TEST_LATEST_REV.getString("_id"), record.key(),
      "should use given document's _id field as key"
    );
    assertEquals(
      TEST_LATEST_REV.getString("_id"), ((Map) record.value()).get("_id"),
      "should use given document as value (_id check)"
    );
    assertEquals(
      TEST_LATEST_REV.getString("_rev"), ((Map) record.value()).get("_rev"),
      "should use given document as value (_rev check)"
    );
    assertEquals(
      TEST_LATEST_REV.getString("bar"), ((Map) record.value()).get("bar"),
      "should use given document as value (bar check)"
    );
  }

  @Test
  public void accumulateJsonObjectsTest() {
    CouchDBSourceTask sourceTask = new CouchDBSourceTask();

    CouchDBSourceTask.Acc acc1 = sourceTask
      .accumulateJsonObjects(new CouchDBSourceTask.Acc(), "{\"incomplete\":\"object\"");
    assertNull(
      acc1.getObj(),
      "should not contain an object after being given an incomplete JSON String"
    );
    assertFalse(
      acc1.hasObject(),
      "should return false for hasObject after being given an incomplete JSON String"
    );
    assertEquals(
      "{\"incomplete\":\"object\"", acc1.str,
      "should store the given incomplete JSON string for further processing"
    );

    CouchDBSourceTask.Acc acc2 = sourceTask
      .accumulateJsonObjects(acc1, ",\"remains\":\"ofObject\"}\n{\"next\":\"object\"");

    assertTrue(
      acc2.hasObject(),
      "should return true for hasObject after being given the remainder of a JSON object as String"
    );
    assertEquals(
      "{\"next\":\"object\"", acc2.str,
      "should contain next incomplete JSON object after parsing previous complete one"
    );
    assertEquals(
      "{\"incomplete\":\"object\",\"remains\":\"ofObject\"}", acc2.getObj().encode(),
      "should contain JSON object after being given the remainder of a JSON object as String"
    );

    CouchDBSourceTask.Acc acc3 = sourceTask
      .accumulateJsonObjects(acc2, "}");
    CouchDBSourceTask.Acc acc4 = sourceTask
      .accumulateJsonObjects(acc3, "\n");

    assertTrue(
      acc4.hasObject(),
      "should return true for hasObject after only end of line character is given"
    );
    assertEquals(
      "", acc4.str,
      "should contain empty String after only end of line character is given"
    );
    assertEquals(
      "{\"next\":\"object\"}", acc4.getObj().encode(),
      "should contain JSON object after only end of line character is given"
    );
  }

  @Test
  public void isNotDesignDocumentTest() {
    CouchDBSourceTask sourceTask = new CouchDBSourceTask();

    JsonObject designDoc = new JsonObject();
    designDoc.put("id", "_design/some-design-doc-id");

    JsonObject nonDesignDoc = new JsonObject();
    nonDesignDoc.put("id", "some-non-design-doc-id");

    assertFalse(
      sourceTask.isNotDesignDocument(designDoc),
      "should return false when a document is a design document"
    );
    assertTrue(
      sourceTask.isNotDesignDocument(nonDesignDoc),
      "should return true when a document is not a design document"
    );
  }
}
