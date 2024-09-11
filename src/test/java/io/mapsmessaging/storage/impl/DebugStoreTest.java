/*
 *   Copyright [2020 - 2024]   [Matthew Buckton]
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 */

package io.mapsmessaging.storage.impl;

import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.storage.StorageBuilder;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class DebugStoreTest extends BaseStoreTest {

  @Override
  public Storage<MappedData> createStore(String testName, boolean sync) throws IOException {
    return build(testName, sync);
  }

  @Test
  @Override
  void basicOpenCloseOpen() {

  }

  public static Storage<MappedData> build(String testName, boolean sync) throws IOException {
    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("Sync", "" + sync);
    properties.put("debug", "true");
    StorageBuilder<MappedData> storageBuilder = new StorageBuilder<>();
    storageBuilder.setStorageType("Memory")
        .setFactory(getFactory())
        .setCache()
        .setName(testName)
        .setProperties(properties);
    return storageBuilder.build();
  }

}
