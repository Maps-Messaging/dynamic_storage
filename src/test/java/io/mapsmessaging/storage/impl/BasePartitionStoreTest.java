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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.Map;

public class BasePartitionStoreTest extends BaseStoreTest {

  @Override
  public Storage<MappedData> createStore(String testName, boolean sync) throws IOException {
    return build(testName, sync);
  }

  public static Map<String, String> buildProperties(boolean sync) throws IOException {
    File file = new File("test_file" + File.separator);
    if (!file.exists()) {
      Files.createDirectory(file.toPath());
    }
    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("Sync", "" + sync);
    properties.put("ItemCount", "" + 100);
    properties.put("debug", "false");
    properties.put("MaxPartitionSize", "" + (512L * 1024L * 1024L)); // set to 5MB data limit
    return properties;
  }

  static Storage<MappedData> build(Map<String, String> properties, String testName) throws IOException {
    StorageBuilder<MappedData> storageBuilder = new StorageBuilder<>();
    storageBuilder.setStorageType("Partition")
        .setFactory(getFactory())
        .setName("test_file" + File.separator + testName)
        .setProperties(properties);
    return storageBuilder.build();
  }

  static Storage<MappedData> build(String testName, boolean sync) throws IOException {
    Map<String, String> properties = buildProperties(sync);
    return build(properties, testName);
  }

}