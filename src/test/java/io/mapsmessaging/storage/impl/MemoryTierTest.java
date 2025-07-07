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

import io.mapsmessaging.storage.ExpiredStorableHandler;
import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.storage.StorageBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

public class MemoryTierTest extends BaseStoreTest {

  public static Storage<MappedData> build(String testName, boolean sync) throws IOException {
    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("Sync", "" + sync);
    StorageBuilder<MappedData> storageBuilder = new StorageBuilder<>();
    storageBuilder.setStorageType("MemoryTier")
        .setFactory(getFactory())
        .setCache()
        .setName(testName)
        .setProperties(properties);
    return storageBuilder.build();
  }

  @Override
  public Storage<MappedData> createStore(String testName, boolean sync) throws IOException {
    return build(testName, sync);
  }


  @Test
  void sizeLimitTest() throws IOException {
    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("Sync", "false");
    properties.put("Tier1Size", "100");
    StorageBuilder<MappedData> storageBuilder = new StorageBuilder<>();
    storageBuilder.setStorageType("MemoryTier")
        .setFactory(getFactory())
        .setCache()
        .setName(testName)
        .setProperties(properties);
    Storage<MappedData> storage = storageBuilder.build();
    try {
      for (int x = 0; x < 100; x++) {
        storage.add(createMessageBuilder(x));
      }

      // There should be zero events in the file but 100 in the memory tier

      for (int x = 100; x < 200; x++) {
        storage.add(createMessageBuilder(x));
      }
      // Now there should be 100 in both
      storage.size();
    } finally {
      storage.delete();
    }
  }

  @Test
  void memoryTierCapacityEvictionTest() throws IOException {
    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("Sync", "false");
    properties.put("Capacity", "10"); // enforce max size of 10 in memory

    AtomicLong expired = new AtomicLong();
    StorageBuilder<MappedData> storageBuilder = new StorageBuilder<>();
    storageBuilder.setStorageType("MemoryTier")
        .setFactory(getFactory())
        .setCache()
        .setName(testName)
        .setExpiredHandler(new ExpiredStorableHandler() {
          @Override
          public void expired(Queue<Long> listOfExpiredEntries) throws IOException {
            expired.incrementAndGet();
          }
        })
        .setProperties(properties);

    Storage<MappedData> storage = storageBuilder.build();
    try {
      // Add more than capacity
      for (int i = 0; i < 25; i++) {
        storage.add(createMessageBuilder(i));
      }

      Assertions.assertEquals(15, expired.get());
      // Verify that only 10 messages remain
      Assertions.assertEquals(10, storage.size(), "Storage should retain only the latest 10 messages");

      // Old keys should be gone
      for (int i = 0; i < 15; i++) {
        Assertions.assertFalse(storage.contains(i), "Key " + i + " should have been evicted due to capacity");
      }

      // New keys should still exist
      for (int i = 15; i < 25; i++) {
        Assertions.assertTrue(storage.contains(i), "Key " + i + " should be present");
      }
    } finally {
      storage.delete();
    }
  }

}
