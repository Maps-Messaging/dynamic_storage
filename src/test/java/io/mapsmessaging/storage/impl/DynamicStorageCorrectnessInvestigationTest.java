/*
 *
 * Copyright [ 2024 - 2026 ] MapsMessaging B.V.
 *
 * Licensed under the Apache License, Version 2.0 with the Commons Clause
 * (the "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *     https://commonsclause.com/
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package io.mapsmessaging.storage.impl;

import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.storage.StorageBuilder;
import io.mapsmessaging.storage.impl.memory.MemoryStorage;
import io.mapsmessaging.storage.impl.memory.MemoryStorageConfig;
import io.mapsmessaging.storage.impl.tier.memory.MemoryTierStorage;
import io.mapsmessaging.utilities.threads.tasks.ThreadLocalContext;
import io.mapsmessaging.utilities.threads.tasks.ThreadStateContext;
import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DynamicStorageCorrectnessInvestigationTest extends BaseTest {

  @Test
  void removingAnIdPresentInBothTiersRemovesBothCopies(@TempDir Path directory) throws IOException {
    Map<String, String> properties = partitionProperties("MemoryTier");
    properties.put("Tier1Size", "2");
    properties.put("MigrationPeriod", "-1000");
    properties.put("ScanInterval", "60000");

    Storage<MappedData> storage = buildStorage(directory.resolve("tiered"), properties);
    try {
      MemoryTierStorage<MappedData> tier = (MemoryTierStorage<MappedData>) storage;
      tier.add(createMessageBuilder(10L));
      tier.scan();
      Assertions.assertEquals(1, tier.size(), "Fixture must migrate the first record to disk");

      tier.add(createMessageBuilder(10L));
      Assertions.assertEquals(2, tier.size(), "Fixture must contain one copy in each tier");

      Assertions.assertTrue(tier.remove(10L));
      Assertions.assertFalse(tier.contains(10L), "A successful removal must not leave the disk copy behind");
      Assertions.assertNull(tier.get(10L), "Removed ID must not be readable from the secondary tier");
    } finally {
      storage.delete();
    }
  }

  @Test
  void negativeIndexedIdSurvivesReadAndReopen(@TempDir Path directory) throws IOException {
    Map<String, String> properties = partitionProperties("Partition");
    StorageBuilder<MappedData> builder = storageBuilder(directory.resolve("negative"), properties);
    Storage<MappedData> storage = builder.build();

    MappedData beforeReopen;
    List<Long> beforeKeys;
    boolean beforeContains;
    try {
      storage.add(createMessageBuilder(-2L));
      beforeContains = storage.contains(-2L);
      beforeKeys = storage.getKeys();
      beforeReopen = storage.get(-2L);
      storage.close();
    } catch (IOException | RuntimeException e) {
      storage.delete();
      throw e;
    }

    Storage<MappedData> reopened = builder.build();
    try {
      Assertions.assertAll("Negative ID read contract",
          () -> Assertions.assertTrue(beforeContains, "Inserted negative key must be indexed"),
          () -> Assertions.assertTrue(beforeKeys.contains(-2L), "Inserted negative key must appear in getKeys"),
          () -> Assertions.assertNotNull(beforeReopen, "Inserted negative key must be readable"),
          () -> Assertions.assertTrue(reopened.contains(-2L), "Reopened index must contain the negative key"),
          () -> Assertions.assertNotNull(reopened.get(-2L), "Negative key must be readable after reopen"));
    } finally {
      reopened.delete();
    }
  }

  @Test
  void rejectingImmutableKeepListMustNotPartiallyDeleteMemoryRecords() throws IOException {
    MemoryStorageConfig config = new MemoryStorageConfig();
    config.setCapacity(-1);
    config.setExpiredEventPoll(-1);
    MemoryStorage<MappedData> storage = new MemoryStorage<>(null, config);
    try {
      storage.add(createMessageBuilder(0L));
      storage.add(createMessageBuilder(1L));

      Assertions.assertThrows(UnsupportedOperationException.class,
          () -> storage.keepOnly(List.of(1L, 2L)));

      Assertions.assertNotNull(storage.get(0L),
          "If immutable input is rejected, the operation must not already have deleted other records");
      Assertions.assertNotNull(storage.get(1L));
    } finally {
      storage.delete();
    }
  }

  @Test
  void rejectingImmutableKeepListMustNotPartiallyDeleteIndexedRecords(@TempDir Path directory) throws IOException {
    Storage<MappedData> storage = buildStorage(directory.resolve("immutable"), partitionProperties("Partition"));
    try {
      storage.add(createMessageBuilder(0L));
      storage.add(createMessageBuilder(1L));

      Assertions.assertThrows(UnsupportedOperationException.class,
          () -> storage.keepOnly(List.of(1L, 2L)));

      Assertions.assertNotNull(storage.get(0L),
          "If immutable input is rejected, the operation must not already have deleted other records");
      Assertions.assertNotNull(storage.get(1L));
    } finally {
      storage.delete();
    }
  }

  private Storage<MappedData> buildStorage(Path location, Map<String, String> properties) throws IOException {
    return storageBuilder(location, properties).build();
  }

  private StorageBuilder<MappedData> storageBuilder(Path location, Map<String, String> properties) {
    ThreadStateContext context = new ThreadStateContext();
    context.add("domain", "ResourceAccessKey");
    ThreadLocalContext.set(context);
    StorageBuilder<MappedData> builder = new StorageBuilder<>();
    builder.setFactory(getFactory()).setName(location.toString()).setProperties(properties);
    return builder;
  }

  private Map<String, String> partitionProperties(String storeType) {
    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("storeType", storeType);
    properties.put("Sync", "false");
    properties.put("ItemCount", "4");
    properties.put("MaxPartitionSize", "1048576");
    properties.put("ExpiredEventPoll", "-1");
    return properties;
  }
}
