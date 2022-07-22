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

package io.mapsmessaging.storage.impl.tier.memory;

import io.mapsmessaging.storage.ExpiredStorableHandler;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.StorableFactory;
import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.storage.impl.BaseStorageFactory;
import io.mapsmessaging.storage.impl.file.PartitionStorageFactory;
import io.mapsmessaging.storage.impl.memory.MemoryFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MemoryTierFactory<T extends Storable> extends BaseStorageFactory<T> {

  private static final long MIGRATION_TIME = 60_000;
  private static final long SCAN_INTERVAL = 10_000;
  private static final long TIER_1_SIZE = 0;

  private final PartitionStorageFactory<T> partitionStorageFactory;
  private final MemoryFactory<ObjectMonitor<T>> memoryFactory;

  public MemoryTierFactory() {
    partitionStorageFactory = null;
    memoryFactory = null;
  }

  public MemoryTierFactory(Map<String, String> properties, StorableFactory<T> storableFactory, ExpiredStorableHandler expiredHandler) {
    super(properties, storableFactory, expiredHandler);
    partitionStorageFactory = new PartitionStorageFactory<>(properties, storableFactory, expiredHandler);
    memoryFactory = new MemoryFactory<>(properties, new ObjectMonitorFactory<>(), expiredHandler);
  }

  @Override
  public String getName() {
    return "MemoryTier";
  }

  @SuppressWarnings("java:S2095") // The allocation of both stores is required and can not be closed in a "finally" clause here, else bad things will happen
  @Override
  public Storage<T> create(String name) throws IOException {
    if (memoryFactory == null || partitionStorageFactory == null) {
      throw new IOException("Uninitialised factory being used.. not supported");
    }
    long migrationTime = MIGRATION_TIME;
    if (properties.containsKey("MigrationPeriod")) {
      migrationTime = Long.parseLong(properties.get("MigrationPeriod"));
    }

    long scanInterval = SCAN_INTERVAL;
    if (properties.containsKey("ScanInterval")) {
      scanInterval = Long.parseLong(properties.get("ScanInterval"));
    }

    long maximumCount = TIER_1_SIZE;
    if (properties.containsKey("Tier1Size")) {
      maximumCount = Long.parseLong(properties.get("Tier1Size"));
    }

    Storage<ObjectMonitor<T>> memoryStorage = memoryFactory.create(name);
    Storage<T> fileStorage = partitionStorageFactory.create(name, memoryStorage.getTaskScheduler());

    return new MemoryTierStorage<>(memoryStorage, fileStorage, scanInterval, migrationTime, maximumCount);
  }

  @Override
  public List<Storage<T>> discovered() {
    return new ArrayList<>();
  }

}
