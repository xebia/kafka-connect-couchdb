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

import com.xebia.kafka.connect.couchdb.converting.JSONConverter;
import com.xebia.kafka.connect.couchdb.merging.LatestWinsMerger;
import io.vertx.core.http.HttpClientOptions;
import org.junit.Test;

import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class CouchDBConnectorConfigTest {
  private CouchDBConnectorConfig config = TestUtils.createConfig();

  @Test
  public void getTaskConfigsTest() {
    int maxTasks = 3;

    List<Map<String, String>> taskConfigs = config.getTaskConfigs(maxTasks);

    assertEquals(
      "produced task configs have same size as max tasks",
      taskConfigs.size(), maxTasks
    );
    assertTrue(
      "produced task configs all contain given config entries",
      taskConfigs.stream().allMatch(map -> map.get("host").equals("127.0.0.1"))
    );
  }

  @Test
  public void getMappingTest() {
    Map<String, String> mapping = config.getMapping("foo/bar,bar/baz");

    assertEquals(
      "2 map entries should have been created",
      mapping.size(), 2
    );
    assertEquals(
      "have correct value for first entry",
      mapping.get("foo"), "bar"
    );
    assertEquals(
      "have correct value for second entry",
      mapping.get("bar"), "baz"
    );

    mapping = config.getMapping("not a mapping at all");

    assertEquals(
      "no map entries should have been created for invalid mapping string",
      mapping.size(), 0
    );
  }

  @Test
  public void getTopicsToDatabasesMappingTest() {
    Map<String, String> mapping = config.getTopicsToDatabasesMapping();

    assertEquals(
      "1 map entry should have been created",
      mapping.size(), 1
    );
    assertEquals(
      "have correct value for first entry",
      mapping.get("couchdb-example"), "couchdb-example"
    );
  }

  @Test
  public void getHttpClientOptionsTest() {
    HttpClientOptions httpClientOptions = config.getHttpClientOptions();

    assertEquals(
      "should use the provided host value as default host",
      httpClientOptions.getDefaultHost(), "127.0.0.1"
    );
    assertEquals(
      "should use the provided port value as default port",
      httpClientOptions.getDefaultPort(), 5984
    );
    assertFalse(
      "should not activate ssl when option set to false",
      httpClientOptions.isSsl()
    );

    CouchDBConnectorConfig config = TestUtils.createConfig(
      "ssl=true",
      "ssl-truststore-path=foo.bar.baz",
      "ssl-truststore-password=MySuperSecretPassword"
    );

    httpClientOptions = config.getHttpClientOptions();

    assertTrue(
      "should activate ssl when option set to true",
      httpClientOptions.isSsl()
    );
    assertEquals(
      "should use the provided trust store path",
      httpClientOptions.getTrustStoreOptions().getPath(), "foo.bar.baz"
    );
    assertEquals(
      "should use the provided trust store password",
      httpClientOptions.getTrustStoreOptions().getPassword(), "MySuperSecretPassword"
    );
  }

  @Test
  public void getBasicAuthTest() {
    String auth = config.getBasicAuth();

    assertEquals("when no user/pass is given auth should not contain them",
      auth, "Basic "
    );

    CouchDBConnectorConfig config = TestUtils.createConfig(
      "username=foo",
      "password=bar"
    );

    auth = config.getBasicAuth();
    String encoded = Base64.getEncoder().encodeToString("foo:bar".getBytes());

    assertEquals("when user/pass are given auth should contain them Base64 encoded",
      auth, "Basic " + encoded
    );
  }

  @Test
  public void getConverterTest() {
    assertTrue(
      "a JSONConverter instance should be created",
      config.getConverter() instanceof JSONConverter
    );
  }

  @Test
  public void getMergerTest() {
    assertTrue(
      "a LatestWinsMerger instance should be created",
      config.getMerger() instanceof LatestWinsMerger
    );
  }
}
