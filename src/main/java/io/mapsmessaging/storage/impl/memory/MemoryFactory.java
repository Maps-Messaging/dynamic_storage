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

package io.mapsmessaging.storage.impl.memory;

import io.mapsmessaging.storage.*;
import io.mapsmessaging.storage.impl.BaseStorageFactory;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MemoryFactory<T extends Storable> extends BaseStorageFactory<T> {

  private static final int EXPIRED_EVENT_MONITOR_TIME = 1;

  public MemoryFactory() {
  }

  public MemoryFactory(Map<String, String> properties, StorableFactory<T> storableFactory, ExpiredStorableHandler expiredHandler) {
    super(properties, storableFactory, expiredHandler);
  }

  @Override
  public String getName() {
    return "Memory";
  }

  @Override
  public StorageFactory<T> getInstance(@NotNull Map<String, String> properties, @NotNull StorableFactory<T> storableFactory, @Nullable ExpiredStorableHandler expiredHandler) {
    return new MemoryFactory<>(properties, storableFactory, expiredHandler);
  }

  @Override
  public Storage<T> create(String name) {
    int expiredEventPoll = EXPIRED_EVENT_MONITOR_TIME;
    if (properties.containsKey("ExpiredEventPoll")) {
      expiredEventPoll = Integer.parseInt(properties.get("ExpiredEventPoll"));
    }
    return new MemoryStorage<>(expiredHandler, expiredEventPoll);
  }

  @Override
  public List<Storage<T>> discovered() {
    return new ArrayList<>();
  }

}
