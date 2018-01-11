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

import java.util.List;

public class MergeResult {
  private JsonObject winner;
  private List<JsonObject> losers;

  public MergeResult(JsonObject winner, List<JsonObject> losers) {
    this.winner = winner;
    this.losers = losers;
  }

  public JsonObject getWinner() {
    return winner;
  }

  public List<JsonObject> getLosers() {
    return losers;
  }
}
