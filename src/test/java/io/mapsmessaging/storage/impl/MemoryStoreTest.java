/*
 *
 *  Copyright [ 2020 - 2024 ] Matthew Buckton
 *  Copyright [ 2024 - 2025 ] MapsMessaging B.V.
 *
 *  Licensed under the Apache License, Version 2.0 with the Commons Clause
 *  (the "License"); you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at:
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *      https://commonsclause.com/
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.mapsmessaging.storage.impl;

import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.storage.StorageBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MemoryStoreTest extends BaseStoreTest {

  @Override
  public Storage<MappedData> createStore(String testName, boolean sync) throws IOException {
    return build(testName, sync);
  }

  @Test
  @Override
  void basicOpenCloseOpen() {

  }

  @Test
  void expiresRecordAfterTheInitialPoll() throws Exception {
    Storage<MappedData> storage = null;
    try {
      storage = build(testName, false, 1);
      MappedData message = createMessageBuilder(1);
      message.setExpiry(System.currentTimeMillis() + 1500L);
      storage.add(message);

      long timeout = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(4);
      while (!storage.isEmpty() && System.currentTimeMillis() < timeout) {
        TimeUnit.MILLISECONDS.sleep(50);
      }

      Assertions.assertTrue(storage.isEmpty(),
          "The record should expire on a poll after the initial scan");
    } finally {
      if (storage != null) {
        storage.delete();
      }
    }
  }

  public static Storage<MappedData> build(String testName, boolean sync) throws IOException {
    return build(testName, sync, 1);
  }

  private static Storage<MappedData> build(String testName, boolean sync, int expiredEventPoll) throws IOException {
    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("Sync", "" + sync);
    properties.put("storeType", "Memory");
    properties.put("ExpiredEventPoll", Integer.toString(expiredEventPoll));
    StorageBuilder<MappedData> storageBuilder = new StorageBuilder<>();
    storageBuilder
        .setFactory(getFactory())
        .setCache()
        .setName(testName)
        .setProperties(properties);
    return storageBuilder.build();
  }

}
