/*
 *   Copyright [2020 - 2022]   [Matthew Buckton]
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

import io.mapsmessaging.storage.AsyncStorage;
import io.mapsmessaging.storage.Statistics;
import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.storage.StorageBuilder;
import io.mapsmessaging.storage.StorageStatistics;
import io.mapsmessaging.utilities.threads.tasks.ThreadLocalContext;
import io.mapsmessaging.utilities.threads.tasks.ThreadStateContext;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PartitionStoreTest extends BaseStoreTest {

  static Storage<MappedData> build(String testName, boolean sync) throws IOException {
    File file = new File("test_file" + File.separator);
    if (!file.exists()) {
      Files.createDirectory(file.toPath());
    }
    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("Sync", "" + sync);
    properties.put("ItemCount", ""+ 100);
    properties.put("MaxPartitionSize", "" + (512L * 1024L * 1024L)); // set to 5MB data limit
    StorageBuilder<MappedData> storageBuilder = new StorageBuilder<>();
    storageBuilder.setStorageType("Partition")
        .setFactory(getFactory())
        .setName("test_file" + File.separator + testName)
        .setProperties(properties);

    return storageBuilder.build();
  }

  @Override
  public Storage<MappedData> createStore(String testName, boolean sync) throws IOException {
    return build(testName, sync);
  }

  @Test
  void testIndexCompaction() throws IOException, ExecutionException, InterruptedException {
    File file = new File("test_file" + File.separator + "testIndexCompaction");
    Files.deleteIfExists(file.toPath());

    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("Sync", "" + false);
    properties.put("ItemCount", ""+ 1_000_000);
    properties.put("MaxPartitionSize", "" + (1024L * 1024L)); // set to 1MB data limit // force the index
    StorageBuilder<MappedData> storageBuilder = new StorageBuilder<>();
    storageBuilder.setStorageType("Partition")
        .setFactory(getFactory())
        .setName("test_file" + File.separator + "testIndexCompaction")
        .setProperties(properties);
    AsyncStorage<MappedData> storage = new AsyncStorage<>(storageBuilder.build());

    ThreadStateContext context = new ThreadStateContext();
    context.add("domain", "ResourceAccessKey");
    ThreadLocalContext.set(context);
    // Remove any before we start

    try {
      for (int x = 0; x < 10_000; x++) {
        MappedData message = createMessageBuilder(x);
        storage.add(message, null).get();
        Assertions.assertEquals(x+1, storage.size().get());
        MappedData lookup = storage.get(x).get();
        Assertions.assertNotNull(lookup);
        Assertions.assertEquals(message.key, lookup.key);
      }
      // We should have compacted the index and have multiple indexes now
      Statistics statistics = storage.getStatistics().get();
      Assertions.assertEquals(26, ((StorageStatistics) statistics).getPartitionCount());
      List<Long> keys = storage.getKeys().get();
      long index =0;
      for(Long key:keys){
        Assertions.assertEquals(index, key);
        index++;
      }
      Assertions.assertEquals(10_000, keys.size());

    } finally {
      storage.delete().get();
    }

  }
}
