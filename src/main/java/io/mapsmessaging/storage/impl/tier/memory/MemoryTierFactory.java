/*
 *
 *  Copyright [ 2020 - 2024 ] Matthew Buckton
 *  Copyright [ 2024 - 2025 ] MapsMessaging B.V.
 *
 *  Licensed under the Apache License, Version 2.0 with the Commons Clause
 *  (the "License"); you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at:
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *      https://commonsclause.com/
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.mapsmessaging.storage.impl.tier.memory;

import io.mapsmessaging.storage.*;
import io.mapsmessaging.storage.impl.BaseStorageFactory;
import io.mapsmessaging.storage.impl.file.PartitionStorageFactory;
import io.mapsmessaging.storage.impl.memory.MemoryFactory;
import io.mapsmessaging.storage.impl.memory.MemoryStorageConfig;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MemoryTierFactory<T extends Storable> extends BaseStorageFactory<T> {

  private final PartitionStorageFactory<T> partitionStorageFactory;
  private final MemoryFactory<ObjectMonitor<T>> memoryFactory;

  public MemoryTierFactory() {
    partitionStorageFactory = null;
    memoryFactory = null;
  }

  public MemoryTierFactory(MemoryTierConfig config, StorableFactory<T> storableFactory, ExpiredStorableHandler expiredHandler) {
    super(config, storableFactory, expiredHandler);
    partitionStorageFactory = new PartitionStorageFactory<>(config.getPartitionStorageConfig(), storableFactory, expiredHandler);
    MemoryStorageConfig storageConfig = config.getMemoryStorageConfig();
    memoryFactory = new MemoryFactory<>(storageConfig, new ObjectMonitorFactory<>(), expiredHandler);
  }

  @Override
  public String getName() {
    return "MemoryTier";
  }

  @Override
  public StorageFactory<T> getInstance(@NotNull StorageConfig config, @NotNull StorableFactory<T> storableFactory, @Nullable ExpiredStorableHandler expiredHandler) {
    return new MemoryTierFactory<>((MemoryTierConfig)config, storableFactory, expiredHandler);
  }

  @SuppressWarnings("java:S2095") // The allocation of both stores is required and can not be closed in a "finally" clause here, else bad things will happen
  @Override
  public Storage<T> create(String name) throws IOException {
    if (memoryFactory == null || partitionStorageFactory == null) {
      throw new IOException("Uninitialised factory being used.. not supported");
    }


    Storage<ObjectMonitor<T>> memoryStorage = memoryFactory.create(name);
    Storage<T> fileStorage = partitionStorageFactory.create(name, memoryStorage.getTaskScheduler());
    MemoryTierConfig memoryTierConfig = (MemoryTierConfig)config;
    return new MemoryTierStorage<>(memoryStorage, fileStorage, memoryTierConfig);
  }

  @Override
  public List<Storage<T>> discovered() {
    return new ArrayList<>();
  }

}
