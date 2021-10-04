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
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PartitionStoreTest extends BaseStoreTest{

  @Override
  public Storage<MappedData> createStore(String testName, boolean sync) throws IOException {
    File file = new File("test_file"+ File.separator);
    if(!file.exists()) {
      Files.createDirectory(file.toPath());
    }
    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("Sync", ""+sync);
    properties.put("MaxPartitionSize", ""+(512L*1024L*1024L)); // set to 5MB data limit
    StorageBuilder<MappedData> storageBuilder = new StorageBuilder<>();
    storageBuilder.setStorageType("Partition")
        .setFactory(getFactory())
        .setName("test_file"+ File.separator+testName)
        .setProperties(properties);

    return storageBuilder.build();
  }


  @Override
  public AsyncStorage<MappedData> createAsyncStore(String testName, boolean sync) throws IOException {
    File file = new File("test_file"+ File.separator);
    if(!file.exists()) {
      Files.createDirectory(file.toPath());
    }
    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("Sync", ""+sync);
    properties.put("MaxPartitionSize", ""+(512L*1024L*1024L)); // set to 5MB data limit
    StorageBuilder<MappedData> storageBuilder = new StorageBuilder<>();
    storageBuilder.setStorageType("Partition")
        .setFactory(getFactory())
        .setName("test_file"+ File.separator+testName)
        .setProperties(properties);
    return storageBuilder.buildAsync();
  }

  @Test
  public void testIndexCompaction() throws IOException, ExecutionException, InterruptedException {
    File file = new File("test_file"+ File.separator+"testIndexCompaction");
    Files.deleteIfExists(file.toPath());

    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("Sync", ""+false);
    properties.put("MaxPartitionSize", ""+(1024L*1024L)); // set to 1MB data limit // force the index
    StorageBuilder<MappedData> storageBuilder = new StorageBuilder<>();
    storageBuilder.setStorageType("Partition")
        .setFactory(getFactory())
        .setName("test_file"+ File.separator+"testIndexCompaction")
        .setProperties(properties);
    AsyncStorage<MappedData> storage = storageBuilder.buildAsync();

    ThreadStateContext context = new ThreadStateContext();
    context.add("domain", "ResourceAccessKey");
    ThreadLocalContext.set(context);
    // Remove any before we start

    try {
      for (int x = 0; x < 1000; x++) {
        MappedData message = createMessageBuilder(x);
        storage.add(message, null).get();
      }
      // We should have compacted the index and have multiple indexes now
      Statistics statistics=storage.getStatistics().get();
      Assertions.assertEquals(3, ((StorageStatistics)statistics).getPartitionCount());
    } finally {
      storage.delete().get();
    }

  }
}
