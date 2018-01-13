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

import java.util.ArrayList;
import java.util.List;

public interface Merger {
  static List<JsonObject> process(MergeResult mergeResult) {
    List<JsonObject> allDocs = new ArrayList<>();
    // Set all losing documents to deleted
    mergeResult.getLosers().forEach(l -> l.put("_deleted", true));
    // Add losing documents to allDocs collection
    allDocs.addAll(mergeResult.getLosers());
    // Add the winner to the collection
    allDocs.add(mergeResult.getWinner());
    // Return all deleted documents and winning document in one collection
    return allDocs;
  }

  MergeResult merge(JsonObject newDoc, JsonObject latestRev, List<JsonObject> conflictingDocs);
}
