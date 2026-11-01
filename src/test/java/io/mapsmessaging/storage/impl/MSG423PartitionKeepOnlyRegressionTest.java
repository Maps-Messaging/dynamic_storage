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
import io.mapsmessaging.storage.StorageStatistics;
import io.mapsmessaging.utilities.threads.tasks.ThreadLocalContext;
import io.mapsmessaging.utilities.threads.tasks.ThreadStateContext;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MSG423PartitionKeepOnlyRegressionTest extends BaseTest {

  @Test
  void keepOnlyDoesNotDeleteRequestedIdsInLaterPartition(@TempDir Path tempDir) throws IOException {
    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("storeType", "Partition");
    properties.put("Sync", "false");
    properties.put("ItemCount", "2");
    properties.put("MaxPartitionSize", "1048576");
    properties.put("ExpiredEventPoll", "-1");

    StorageBuilder<MappedData> builder = new StorageBuilder<>();
    builder.setFactory(getFactory())
        .setName(tempDir.resolve("keep-only-partitions").toString())
        .setProperties(properties);

    ThreadStateContext context = new ThreadStateContext();
    context.add("domain", "ResourceAccessKey");
    ThreadLocalContext.set(context);

    Storage<MappedData> storage = builder.build();
    try {
      for (long key = 0; key < 4; key++) {
        storage.add(createMessageBuilder(key));
      }

      Assertions.assertEquals(4, storage.size(), "Fixture must contain four records");
      Assertions.assertEquals(2, ((StorageStatistics) storage.getStatistics()).getPartitionCount(),
          "Fixture must span exactly two partitions");

      // The first partition holds 0,1; only the second holds requested keys 2,3.
      // Removing two unwanted keys must not empty the keep-list merely because
      // the number removed equals the number requested.
      Collection<Long> unmatched = storage.keepOnly(new ArrayList<>(List.of(2L, 3L)));

      Assertions.assertAll("keepOnly must retain both requested records",
          () -> Assertions.assertEquals(List.of(), new ArrayList<>(unmatched),
              "Both requested keys were present in the original store"),
          () -> Assertions.assertEquals(2, storage.size(), "Exactly two records must remain"),
          () -> Assertions.assertEquals(List.of(2L, 3L), new ArrayList<>(storage.getKeys()),
              "Both requested keys belong to the later partition"),
          () -> Assertions.assertNull(storage.get(0L)),
          () -> Assertions.assertNull(storage.get(1L)),
          () -> Assertions.assertNotNull(storage.get(2L), "Requested key 2 must not be deleted"),
          () -> Assertions.assertNotNull(storage.get(3L), "Requested key 3 must not be deleted"));
    } finally {
      storage.delete();
    }
  }
}
