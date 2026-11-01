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

import io.mapsmessaging.storage.impl.memory.MemoryStorage;
import io.mapsmessaging.storage.impl.memory.MemoryStorageConfig;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class MSG424MemoryKeepOnlyRegressionTest extends BaseTest {

  @Test
  void disjointKeepListWithEqualSizeReturnsAllUnmatchedIds() throws IOException {
    MemoryStorage<MappedData> storage = newStorage();
    try {
      storage.add(createMessageBuilder(0L));
      storage.add(createMessageBuilder(1L));

      Collection<Long> unmatched = storage.keepOnly(new ArrayList<>(List.of(2L, 3L)));

      Assertions.assertEquals(List.of(2L, 3L), new ArrayList<>(unmatched),
          "Records absent from this store must remain eligible for subsequent storage layers");
      Assertions.assertEquals(0, storage.size());
      Assertions.assertTrue(storage.getKeys().isEmpty());
      Assertions.assertNull(storage.get(0L));
      Assertions.assertNull(storage.get(1L));
    } finally {
      storage.delete();
    }
  }

  @Test
  void partiallyOverlappingKeepListRetainsExistingAndReturnsMissingIds() throws IOException {
    MemoryStorage<MappedData> storage = newStorage();
    try {
      storage.add(createMessageBuilder(0L));
      storage.add(createMessageBuilder(1L));

      Collection<Long> unmatched = storage.keepOnly(new ArrayList<>(List.of(1L, 2L)));

      Assertions.assertEquals(List.of(2L), new ArrayList<>(unmatched));
      Assertions.assertEquals(List.of(1L), storage.getKeys());
      Assertions.assertEquals(1, storage.size());
      Assertions.assertNull(storage.get(0L));
      Assertions.assertNotNull(storage.get(1L));
    } finally {
      storage.delete();
    }
  }

  @Test
  void keepListContainingOnlyExistingIdsRetainsAllRecords() throws IOException {
    MemoryStorage<MappedData> storage = newStorage();
    try {
      storage.add(createMessageBuilder(0L));
      storage.add(createMessageBuilder(1L));

      Collection<Long> unmatched = storage.keepOnly(new ArrayList<>(List.of(0L, 1L)));

      Assertions.assertTrue(unmatched.isEmpty());
      Assertions.assertEquals(List.of(0L, 1L), storage.getKeys());
      Assertions.assertEquals(2, storage.size());
      Assertions.assertNotNull(storage.get(0L));
      Assertions.assertNotNull(storage.get(1L));
    } finally {
      storage.delete();
    }
  }

  private MemoryStorage<MappedData> newStorage() {
    MemoryStorageConfig config = new MemoryStorageConfig();
    config.setCapacity(-1);
    config.setExpiredEventPoll(-1);
    return new MemoryStorage<>(null, config);
  }
}
