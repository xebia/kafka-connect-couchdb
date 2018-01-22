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

import io.vertx.rxjava.core.buffer.Buffer;
import io.vertx.rxjava.ext.web.client.HttpResponse;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CouchDBSinkTaskTest {
  @Mock
  HttpResponse<Buffer> mockResponse;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Test
  public void validateResponseTest() {
    CouchDBSinkTask sinkTask = new CouchDBSinkTask();

    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn(Buffer.buffer("Filled body"));
    assertNotNull(
      sinkTask.validateResponse(mockResponse),
      "should not throw error when response has OK status and has filled body"
    );

    when(mockResponse.statusCode()).thenReturn(409);
    when(mockResponse.body()).thenReturn(Buffer.buffer("Filled body"));
    assertNotNull(
      sinkTask.validateResponse(mockResponse, 409),
      "should not throw error when response has accepted status and has filled body"
    );

    when(mockResponse.statusCode()).thenReturn(500);
    when(mockResponse.body()).thenReturn(Buffer.buffer("Filled body"));
    assertThrows(
      RuntimeException.class, () -> sinkTask.validateResponse(mockResponse, 409),
      "should throw error when response has error status and filled body [1]"
    );

    when(mockResponse.statusCode()).thenReturn(100);
    when(mockResponse.body()).thenReturn(Buffer.buffer("Filled body"));
    assertThrows(
      RuntimeException.class, () -> sinkTask.validateResponse(mockResponse, 409),
      "should throw error when response has error status and filled body [2]"
    );

    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn(Buffer.buffer(""));
    assertThrows(
      RuntimeException.class, () -> sinkTask.validateResponse(mockResponse),
      "should throw error when response has OK status but empty body"
    );

    when(mockResponse.statusCode()).thenReturn(409);
    when(mockResponse.body()).thenReturn(Buffer.buffer(""));
    assertThrows(
      RuntimeException.class, () -> sinkTask.validateResponse(mockResponse, 409),
      "should throw error when response has accepted status but empty body"
    );
  }
}
