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

import com.fasterxml.jackson.databind.DeserializationFeature;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.Vertx;
import io.vertx.rxjava.core.http.HttpServer;
import io.vertx.rxjava.core.http.HttpServerRequest;

import java.util.HashMap;
import java.util.Map;

import static com.xebia.kafka.connect.couchdb.TestUtils.*;

class MockCouchDBServer {
  private boolean insertShouldSucceed;
  private HttpServer server;

  MockCouchDBServer(String host, int port) {
    this.insertShouldSucceed = true;

    HttpServerOptions httpServerOptions = new HttpServerOptions()
      .setHost(host)
      .setPort(port);

    this.server = Vertx.vertx().createHttpServer(httpServerOptions);
  }

  private void handleGET(HttpServerRequest req) {
    if (req.query().contains("conflicts=true")) {
      JsonArray conflicts = new JsonArray();
      conflicts.add(conflict1.getString("_rev"));
      conflicts.add(conflict2.getString("_rev"));

      JsonObject latestRevCopy = latestRev.copy();
      latestRevCopy.put("_conflicts", conflicts);

      req.response().end(latestRevCopy.encode());

    } else if (req.query().contains("rev=")) {
      String rev = req.query().split("=")[1];
      switch (rev) {
        case "1":
          req.response().end(conflict2.copy().encode());
          break;
        case "2":
          req.response().end(conflict1.copy().encode());
          break;
        case "3":
          req.response().end(latestRev.copy().encode());
          break;
      }
    }
  }

  private void handlePOST(HttpServerRequest req, Map<String, JsonObject> postData) {
    req.bodyHandler(buffer -> postData.put(req.path(), buffer.toJsonObject()));

    if ((req.path().contains("_bulk_docs")) || insertShouldSucceed) {
      req.response().setStatusCode(201).end("{\"ok\",true}");
    } else {
      req.response().setStatusCode(409).end("{\"ok\",false}");
    }
  }

  Map<String, JsonObject> start() {
    Map<String, JsonObject> postData = new HashMap<>();

    server.requestHandler(req -> {
      switch (req.method()) {
        case GET:
          handleGET(req);
          break;
        case POST:
          handlePOST(req, postData);
          break;
      }
    });

    server.listen();

    return postData;
  }

  void stop() {
    if (server != null) server.rxClose().toCompletable().await();
  }

  void setInsertShouldSucceed(boolean insertShouldSucceed) {
    this.insertShouldSucceed = insertShouldSucceed;
  }
}
