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

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.xebia.kafka.connect.couchdb.TestUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class CouchDBSinkTaskIT {
  private List<SinkRecord> records;
  private CouchDBSinkTask sinkTask;
  private MockCouchDBServer mockCouchDBServer;
  private Map<String, JsonObject> postData;

  @Before
  public void init() {
    mockCouchDBServer = new MockCouchDBServer(
      "127.0.0.1",
      5984
    );
    postData = mockCouchDBServer.start();

    records = new ArrayList<>();
    records.add(new SinkRecord(
      "MyTopic",
      1,
      Schema.STRING_SCHEMA,
      "MyKey",
      Schema.STRING_SCHEMA,
      newDoc.encode(),
      1
    ));

    Map<String, String> config = TestUtils.createConfigMap();
    config.put("topics-to-databases-mapping", "MyTopic/MyDatabase");
    sinkTask = new CouchDBSinkTask();
    sinkTask.start(config);
  }

  @After
  public void breakDown() {
    mockCouchDBServer.stop();
  }

  @Test
  public void insertTest() {
    mockCouchDBServer.setInsertShouldSucceed(true);

    sinkTask.put(records);

    assertEquals(
      newDoc.encode(), postData.get("/MyDatabase").encode(),
      "should post new document to correct path"
    );
  }

  @Test
  public void updateTest() {
    mockCouchDBServer.setInsertShouldSucceed(false);

    sinkTask.put(records);

    assertEquals(
      postData.get("/MyDatabase").encode(), newDoc.encode(),
      "should try to insert doc"
    );

    JsonObject newDocCopy = newDoc
      .copy()
      .put("_rev", latestRev.getString("_rev"));
    JsonObject latestRevCopy = latestRev
      .copy()
      .put("_conflicts", new JsonArray(Arrays.asList("2", "1")))
      .put("_deleted", true);
    JsonObject conflict1Copy = conflict1
      .copy()
      .put("_deleted", true);
    JsonObject conflict2Copy = conflict2
      .copy()
      .put("_deleted", true);

    JsonArray docs = new JsonArray();
    docs.add(latestRevCopy);
    docs.add(conflict1Copy);
    docs.add(conflict2Copy);
    docs.add(newDocCopy);

    JsonObject expected = new JsonObject();
    expected.put("docs", docs);

    assertEquals(
      expected.encode(), postData.get("/MyDatabase/_bulk_docs").encode(),
      "should post to _bulk_docs a collection containing the newDoc with latest rev appended and " +
        "latestRev plus conflicts marked for deletion"
    );
  }
}
