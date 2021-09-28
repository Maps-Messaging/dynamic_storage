/*
 *
 * Copyright [2020 - 2021]   [Matthew Buckton]
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 *
 */

package io.mapsmessaging.storage.impl;

import io.mapsmessaging.storage.AsyncStorage;
import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.storage.StorageBuilder;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class PartitionStoreTest extends BaseStoreTest{

  private static long testNumber = 0;
  @Override
  public Storage<MappedData> createStore(boolean sync) throws IOException {
    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("Sync", ""+sync);
    StorageBuilder<MappedData> storageBuilder = new StorageBuilder<>();
    storageBuilder.setStorageType("Partition")
        .setFactory(getFactory())
        .setName("Test_"+testNumber++)
        .setProperties(properties);
    return storageBuilder.build();
  }


  @Override
  public AsyncStorage<MappedData> createAsyncStore(boolean sync) throws IOException {
    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("Sync", ""+sync);
    StorageBuilder<MappedData> storageBuilder = new StorageBuilder<>();
    storageBuilder.setStorageType("Partition")
        .setFactory(getFactory())
        .setName("Test_"+testNumber++)
        .setProperties(properties);
    return storageBuilder.buildAsync();
  }
}
