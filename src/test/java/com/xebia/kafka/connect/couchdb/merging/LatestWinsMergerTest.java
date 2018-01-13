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

package com.xebia.kafka.connect.couchdb.merging;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.junit.Test;

import java.util.Arrays;
import static org.junit.Assert.*;

public class LatestWinsMergerTest {
  private LatestWinsMerger lwm = new LatestWinsMerger();

  @Test
  public void mergeTest() {
    JsonObject newDoc = Json.mapper.convertValue("{\"foo\":\"bar\"}", JsonObject.class);
    JsonObject latestRev = Json.mapper.convertValue("{\"bar\":\"baz\"}", JsonObject.class);
    JsonObject conflict1 = Json.mapper.convertValue("{\"baz\":\"foo\"}", JsonObject.class);
    JsonObject conflict2 = Json.mapper.convertValue("{\"bar\":\"foo\"}", JsonObject.class);

    MergeResult mr = lwm.merge(newDoc, latestRev, Arrays.asList(conflict1, conflict2));

    assertEquals(
      "winning doc should be newDoc",
      mr.getWinner(), newDoc
    );
    assertEquals(
      "losing docs should be all others",
      mr.getLosers(), Arrays.asList(latestRev, conflict1, conflict2)
    );
  }
}
