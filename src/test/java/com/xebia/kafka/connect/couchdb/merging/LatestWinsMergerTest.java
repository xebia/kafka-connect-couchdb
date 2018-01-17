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

import org.junit.Test;

import java.util.Arrays;

import static com.xebia.kafka.connect.couchdb.TestUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class LatestWinsMergerTest {
  private LatestWinsMerger lwm = new LatestWinsMerger();

  @Test
  public void mergeTest() {
    MergeResult mr = lwm.merge(TEST_NEW_DOC, TEST_LATEST_REV, Arrays.asList(TEST_CONFLICT1, TEST_CONFLICT2));

    assertEquals(
      TEST_NEW_DOC, mr.getWinner(),
      "winning doc should be TEST_NEW_DOC"
    );
    assertEquals(
      Arrays.asList(TEST_LATEST_REV, TEST_CONFLICT1, TEST_CONFLICT2), mr.getLosers(),
      "losing docs should be all others"
    );
  }
}
