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
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.json.JsonConverter;
import org.junit.Test;

import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class CouchDBConnectorConfigTest {
  private CouchDBConnectorConfig config = TestUtils.createConfig();

  @Test
  public void getTaskConfigsTest() {
    int maxTasks = 3;

    List<Map<String, String>> taskConfigs = config.getTaskConfigs(maxTasks);

    assertEquals(
      maxTasks, taskConfigs.size(),
      "produced task configs have same size as max tasks"
    );
    assertTrue(
      taskConfigs.stream().allMatch(map -> map.get("host").equals("127.0.0.1")),
      "produced task configs all contain given config entries"
    );
  }

  @Test
  public void getListingTest() {
    List<String> listing = config.getListing("foo,bar,baz");

    assertEquals(
      3, listing.size(),
      "should have created 3 items"
    );
    assertEquals(
      "foo", listing.get(0),
      "should have correct value for first entry"
    );
    assertEquals(
      "bar", listing.get(1),
      "should have correct value for second entry"
    );
    assertEquals(
      "baz", listing.get(2),
      "should have correct value for third entry"
    );
  }

  @Test
  public void getTopicsTest() {
    List<String> topics = config.getTopics();

    assertEquals(
      "kafka-example", topics.get(0),
      "should contain the configured topic"
    );
  }

  @Test
  public void getMappingTest() {
    Map<String, String> mapping = config.getMapping("foo/bar,bar/baz");

    assertEquals(
      2, mapping.size(),
      "should have created 2 map entries"
    );
    assertEquals(
      "bar", mapping.get("foo"),
      "should have correct value for first entry"
    );
    assertEquals(
      "baz", mapping.get("bar"),
      "should have correct value for second entry"
    );

    mapping = config.getMapping("not a mapping at all");

    assertEquals(
      0, mapping.size(),
      "should not have created map entries for invalid mapping string"
    );
  }

  @Test
  public void getSinkTopicsToDatabasesMappingTest() {
    Map<String, String> mapping = config.getSinkTopicsToDatabasesMapping();

    assertEquals(
      1, mapping.size(),
      "1 map entry should have been created"
    );
    assertEquals(
      "couchdb-example", mapping.get("kafka-example"),
      "should have correct value for first entry"
    );

    CouchDBConnectorConfig incorrectConfig = TestUtils.createConfig(
      "topics=MyTopic,MyOtherTopic",
      "sink-topics-to-databases-mapping=MyTopic/MyDatabase"
    );

    assertThrows(
      ConfigException.class, incorrectConfig::getSinkTopicsToDatabasesMapping,
      "should fail when not all topics specified have a database mapping"
    );
  }

  @Test
  public void getSourceTopicsToDatabasesMappingTest() {
    Map<String, String> mapping = config.getSourceTopicsToDatabasesMapping();

    assertEquals(
      1, mapping.size(),
      "should have been created 1 map entry"
    );
    assertEquals(
      "couchdb-example", mapping.get("kafka-example"),
      "should have correct value for first entry"
    );

    CouchDBConnectorConfig incorrectConfig = TestUtils.createConfig(
      "topics=MyTopic,MyOtherTopic",
      "source-topics-to-databases-mapping=MyTopic/MyDatabase"
    );

    assertThrows(
      ConfigException.class, incorrectConfig::getSourceTopicsToDatabasesMapping,
      "should fail when not all topics specified have a database mapping"
    );
  }

  @Test
  public void getTopicsToIdFieldsMappingTest() {
    Map<String, String> mapping = config.getTopicsToIdFieldsMapping();

    assertEquals(
      1, mapping.size(),
      "should have been created 1 map entry"
    );
    assertEquals(
      "id-field-example", mapping.get("kafka-example"),
      "should have correct value for first entry"
    );

    CouchDBConnectorConfig incorrectConfig = TestUtils.createConfig(
      "sink-topics-to-databases-mapping=MyTopic/MyDatabase,MyOtherTopic/MyOtherDatabase",
      "topics-to-id-fields-mapping=MyOtherTopic/MyOtherIdField"
    );

    assertThrows(
      ConfigException.class, incorrectConfig::getTopicsToIdFieldsMapping,
      "should fail when not all topics specified for sink have an id field specified"
    );
  }

  @Test
  public void getDatabasesToChangesSinceMappingTest() {
    Map<String, String> mapping = config.getDatabasesToChangesSinceMapping();

    assertEquals(
      1, mapping.size(),
      "should have been created 1 map entry"
    );
    assertEquals(
      "0", mapping.get("couchdb-example"),
      "should have correct value for first entry"
    );
  }

  @Test
  public void getHttpClientOptionsTest() {
    HttpClientOptions httpClientOptions = config.getHttpClientOptions();

    assertEquals(
      "127.0.0.1", httpClientOptions.getDefaultHost(),
      "should use the provided host value as default host"
    );
    assertEquals(
      5984, httpClientOptions.getDefaultPort(),
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
      "foo.bar.baz", httpClientOptions.getTrustStoreOptions().getPath(),
      "should use the provided trust store path"
    );
    assertEquals(
      "MySuperSecretPassword", httpClientOptions.getTrustStoreOptions().getPassword(),
      "should use the provided trust store password"
    );
  }

  @Test
  public void getBasicAuthTest() {
    String auth = config.getBasicAuth();

    assertEquals(
      "Basic ", auth,
      "should not contain user/pass when none are given"
    );

    CouchDBConnectorConfig config = TestUtils.createConfig(
      "username=foo",
      "password=bar"
    );

    auth = config.getBasicAuth();
    String encoded = Base64.getEncoder().encodeToString("foo:bar".getBytes());

    assertEquals(
      "Basic " + encoded, auth,
      "should contains Base64 encoded user/pass when user/pass are given"
    );
  }

  @Test
  public void getConverterTest() {
    assertTrue(
      config.getConverter() instanceof JsonConverter,
      "a JsonConverter instance should be created"
    );

    CouchDBConnectorConfig incorrectConfig = TestUtils.createConfig("converter=some.non.existing.Class");
    assertThrows(
      ConfigException.class, incorrectConfig::getConverter,
      "should throw a configuration exception when converter class given cannot be instantiated"
    );
  }

  @Test
  public void getMergerTest() {
    assertTrue(
      config.getMerger() instanceof LatestWinsMerger,
      "a LatestWinsMerger instance should be created"
    );

    CouchDBConnectorConfig incorrectConfig = TestUtils.createConfig("merger=some.non.existing.Class");
    assertThrows(
      ConfigException.class, incorrectConfig::getMerger,
      "should throw a configuration exception when merger class given cannot be instantiated"
    );
  }
}
