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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
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

  private Storage<MappedData> createCompactionStore() throws IOException {
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
    return storageBuilder.build();
  }

  @Test
  void testCompactionWithTrailingDeletion() throws IOException {
    int eventCount = 10_000;
    int skipCount = 100;
    Storage<MappedData> storage = null;
    try {
      storage = createCompactionStore();
      ThreadStateContext context = new ThreadStateContext();
      context.add("domain", "ResourceAccessKey");
      ThreadLocalContext.set(context);
      // Remove any before we start

      int deleteIndex = 0;
      for (int x = 0; x < eventCount; x++) {
        MappedData message = createMessageBuilder(x);
        validateMessage(message, x);
        storage.add(message);
        if (storage.size() > skipCount) {
          Assertions.assertTrue(storage.remove(deleteIndex), "Failed to delete index " + deleteIndex);
          deleteIndex++;
          if (deleteIndex % skipCount == 0) {
            deleteIndex++; // skip every 500
          }
        }
      }
      while (deleteIndex < eventCount) {
        Assertions.assertTrue(storage.remove(deleteIndex), "Failed to delete index " + deleteIndex);
        deleteIndex++;
        if (deleteIndex % skipCount == 0) {
          deleteIndex++; // skip every 500
        }
      }

      Assertions.assertEquals(skipCount-1, storage.size());

      for (int x = skipCount; x < eventCount; x = x + skipCount) {
        Assertions.assertTrue(storage.contains(x), "Should contain index: " + x);
      }
      long index = skipCount;
      List<Long> keyList = storage.getKeys();
      for (Long key : keyList) {
        Assertions.assertEquals(index, key);
        index += skipCount;
      }
      storage.keepOnly(new ArrayList<>());

      Assertions.assertTrue(storage.isEmpty());
    } finally {
      if (storage != null) {
        storage.delete();
      }
    }
  }

  @Test
  void testIndexCompaction() throws IOException, ExecutionException, InterruptedException {
    AsyncStorage<MappedData> storage = new AsyncStorage<>(createCompactionStore());

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


  @Test
  void testRestart() throws IOException, ExecutionException, InterruptedException {
    File file = new File("test_file" + File.separator + "testRestart");
    Files.deleteIfExists(file.toPath());

    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("Sync", "" + false);
    properties.put("ItemCount", ""+ 1_000);
    properties.put("ExpiredEventPoll", ""+2);
    properties.put("MaxPartitionSize", "" + (1024L * 1024L)); // set to 1MB data limit // force the index
    StorageBuilder<MappedData> storageBuilder = new StorageBuilder<>();
    storageBuilder.setStorageType("Partition")
        .setFactory(getFactory())
        .setName("test_file" + File.separator + "testRestart")
        .setProperties(properties);
    AsyncStorage<MappedData> storage = new AsyncStorage<>(storageBuilder.build());

    ThreadStateContext context = new ThreadStateContext();
    context.add("domain", "ResourceAccessKey");
    ThreadLocalContext.set(context);
    // Remove any before we start

    try {
      for (int x = 0; x < 10_000; x++) {
        MappedData message = createMessageBuilder(x);
        message.setExpiry(System.currentTimeMillis()+2000); // 2 Seconds
        message.setKey(x);
        storage.add(message, null).get();
        Assertions.assertEquals(x+1, storage.size().get());
        MappedData lookup = storage.get(x).get();
        Assertions.assertNotNull(lookup);
        Assertions.assertEquals(message.key, lookup.key);
      }
      storage.close();
      TimeUnit.SECONDS.sleep(3);
      // Now let's reopen the file and check the expired events
      storage = new AsyncStorage<>(storageBuilder.build());
      int count = 0;
      while(storage.size().get() != 0 && count < 20){
        TimeUnit.SECONDS.sleep(1);
        count++;
      }
      Assertions.assertEquals(0, (long) storage.size().get());

    } finally {
      storage.delete().get();
    }
  }
}
