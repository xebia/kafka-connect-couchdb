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

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.xebia.kafka.connect.couchdb.TestUtils.TEST_LATEST_REV;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CouchDBSourceTaskIT {
  private CouchDBSourceTask sourceTask;
  private MockCouchDBServer mockCouchDBServer;

  @Before
  public void init() {
    mockCouchDBServer = new MockCouchDBServer();
    mockCouchDBServer.start();

    Map<String, String> config = TestUtils.createConfigMap();
    config.put("topics", "MyTopic,MyOtherTopic");
    config.put("source-topics-to-databases-mapping", "MyTopic/MyDatabase,MyOtherTopic/MyOtherDatabase");
    sourceTask = new CouchDBSourceTask();
    sourceTask.start(config);
  }

  @After
  public void breakDown() {
    mockCouchDBServer.stop();
  }

  @Test
  public void pollTest() {
    List<SourceRecord> batch = new ArrayList<>();
    try {
      batch = sourceTask.poll();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    Supplier<Stream<SourceRecord>> records = batch::stream;
    Supplier<Stream<Map>> docs = () -> records.get().map(r -> (Map) r.value());

    assertEquals(
      12, batch.size(),
      "batch should not exceed batch size"
    );
    assertTrue(
      records
        .get()
        .map(SourceRecord::topic)
        .allMatch(t -> t.equals("MyTopic") || t.equals("MyOtherTopic")),
      "all records should have correct topic"
    );
    assertTrue(
      records
        .get()
        .map(r -> r.sourcePartition().get("database"))
        .allMatch(dbName -> dbName.equals("MyDatabase") || dbName.equals("MyOtherDatabase")),
      "all records should have correct database"
    );
    assertTrue(
      docs
        .get()
        .allMatch(d -> TEST_LATEST_REV.getString("_id").equals(d.get("_id"))),
      "all docs should have correct _id"
    );
    assertTrue(
      docs
        .get()
        .allMatch(d -> TEST_LATEST_REV.getString("_rev").equals(d.get("_rev"))),
      "all docs should have correct _rev"
    );
  }
}
