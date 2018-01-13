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

import com.xebia.kafka.connect.couchdb.merging.LatestWinsMerger;
import io.vertx.core.http.HttpClientOptions;
import org.apache.kafka.connect.json.JsonConverter;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CouchDBConnectorConfigTest {
  private CouchDBConnectorConfig config = TestUtils.createConfig();

  @Test
  public void getTaskConfigsTest() {
    int maxTasks = 3;

    List<Map<String, String>> taskConfigs = config.getTaskConfigs(maxTasks);

    assertEquals(
      taskConfigs.size(), maxTasks,
      "produced task configs have same size as max tasks"
    );
    assertTrue(
      taskConfigs.stream().allMatch(map -> map.get("host").equals("127.0.0.1")),
      "produced task configs all contain given config entries"
    );
  }

  @Test
  public void getMappingTest() {
    Map<String, String> mapping = config.getMapping("foo/bar,bar/baz");

    assertEquals(
      mapping.size(), 2,
      "2 map entries should have been created"
      );
    assertEquals(
      mapping.get("foo"), "bar",
      "have correct value for first entry"
    );
    assertEquals(
      mapping.get("bar"), "baz",
      "have correct value for second entry"
    );

    mapping = config.getMapping("not a mapping at all");

    assertEquals(
      mapping.size(), 0,
      "no map entries should have been created for invalid mapping string"
    );
  }

  @Test
  public void getTopicsToDatabasesMappingTest() {
    Map<String, String> mapping = config.getTopicsToDatabasesMapping();

    assertEquals(
      mapping.size(), 1,
      "1 map entry should have been created"
    );
    assertEquals(
      mapping.get("couchdb-example"), "couchdb-example",
      "have correct value for first entry"
    );
  }

  @Test
  public void getHttpClientOptionsTest() {
    HttpClientOptions httpClientOptions = config.getHttpClientOptions();

    assertEquals(
      httpClientOptions.getDefaultHost(), "127.0.0.1",
      "should use the provided host value as default host"
    );
    assertEquals(
      httpClientOptions.getDefaultPort(), 5984,
      "should use the provided port value as default port"
    );
    assertFalse(
      httpClientOptions.isSsl(),
      "should not activate ssl when option set to false"
    );

    CouchDBConnectorConfig config = TestUtils.createConfig(
      "ssl=true",
      "ssl-truststore-path=foo.bar.baz",
      "ssl-truststore-password=MySuperSecretPassword"
    );

    httpClientOptions = config.getHttpClientOptions();

    assertTrue(
      httpClientOptions.isSsl(),
      "should activate ssl when option set to true"
    );
    assertEquals(
      httpClientOptions.getTrustStoreOptions().getPath(), "foo.bar.baz",
      "should use the provided trust store path"
    );
    assertEquals(
      httpClientOptions.getTrustStoreOptions().getPassword(), "MySuperSecretPassword",
      "should use the provided trust store password"
    );
  }

  @Test
  public void getBasicAuthTest() {
    String auth = config.getBasicAuth();

    assertEquals(
      auth, "Basic ",
      "when no user/pass is given auth should not contain them"
    );

    CouchDBConnectorConfig config = TestUtils.createConfig(
      "username=foo",
      "password=bar"
    );

    auth = config.getBasicAuth();
    String encoded = Base64.getEncoder().encodeToString("foo:bar".getBytes());

    assertEquals(
      auth, "Basic " + encoded,
      "when user/pass are given auth should contain them Base64 encoded"
    );
  }

  @Test
  public void getConverterTest() {
    assertTrue(
      config.getConverter() instanceof JsonConverter,
      "a JsonConverter instance should be created"
    );
  }

  @Test
  public void getMergerTest() {
    assertTrue(
      config.getMerger() instanceof LatestWinsMerger,
      "a LatestWinsMerger instance should be created"
    );
  }
}
