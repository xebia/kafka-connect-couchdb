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

import io.vertx.core.json.JsonObject;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static com.xebia.kafka.connect.couchdb.TestUtils.*;
import static org.junit.jupiter.api.Assertions.*;

public class MergerTest {
  @Test
  public void processTest() {
    MergeResult mr = new MergeResult(TEST_NEW_DOC, Arrays.asList(TEST_LATEST_REV, TEST_CONFLICT1, TEST_CONFLICT2));
    List<JsonObject> processed = Merger.process(mr);

    assertTrue(
      processed.get(0).getString("bar").equals("baz") && processed.get(0).getBoolean("_deleted"),
      "should mark first element for deletion"
    );
    assertTrue(
      processed.get(1).getString("baz").equals("foo") && processed.get(1).getBoolean("_deleted"),
      "should mark second element for deletion"
    );
    assertTrue(
      processed.get(2).getString("bar").equals("foo") && processed.get(2).getBoolean("_deleted"),
      "should mark third element for deletion"
    );
    assertTrue(
      processed.get(3).getString("foo").equals("bar"),
      "should add winner last"
    );
    assertNull(
      processed.get(3).getBoolean("_deleted"),
      "should not mark winner for deletion"
    );
  }
}
