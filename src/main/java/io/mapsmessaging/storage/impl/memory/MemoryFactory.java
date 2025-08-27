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

package io.mapsmessaging.storage.impl.memory;

import io.mapsmessaging.storage.*;
import io.mapsmessaging.storage.impl.BaseStorageFactory;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class MemoryFactory<T extends Storable> extends BaseStorageFactory<T> {

  public MemoryFactory() {
  }

  public MemoryFactory(MemoryStorageConfig config, StorableFactory<T> storableFactory, ExpiredStorableHandler expiredHandler) {
    super(config, storableFactory, expiredHandler);
  }

  @Override
  public String getName() {
    return "Memory";
  }

  @Override
  public StorageFactory<T> getInstance(@NotNull StorageConfig config, @NotNull StorableFactory<T> storableFactory, @Nullable ExpiredStorableHandler expiredHandler) {
    return new MemoryFactory<>((MemoryStorageConfig)config, storableFactory, expiredHandler);
  }

  @Override
  public Storage<T> create(String name) {
    return new MemoryStorage<>(expiredHandler, (MemoryStorageConfig) config);
  }

  @Override
  public List<Storage<T>> discovered() {
    return new ArrayList<>();
  }

}
