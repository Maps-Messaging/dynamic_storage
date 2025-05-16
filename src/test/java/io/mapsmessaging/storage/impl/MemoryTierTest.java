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
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

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
}
