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

import io.mapsmessaging.storage.BaseExpiredHandler;
import io.mapsmessaging.storage.ExpiredMonitor;
import io.mapsmessaging.storage.ExpiredStorableHandler;
import io.mapsmessaging.storage.Statistics;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.storage.StorageStatistics;
import io.mapsmessaging.storage.impl.expired.ExpireStorableTaskManager;
import io.mapsmessaging.storage.impl.file.TaskQueue;
import io.mapsmessaging.utilities.collections.NaturalOrderedLongList;
import io.mapsmessaging.utilities.collections.NaturalOrderedLongQueue;
import io.mapsmessaging.utilities.collections.bitset.BitSetFactory;
import io.mapsmessaging.utilities.collections.bitset.BitSetFactoryImpl;
import io.mapsmessaging.utilities.threads.tasks.TaskScheduler;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;

public class MemoryStorage<T extends Storable> implements Storage<T>, ExpiredMonitor {

  private static final AtomicLong counter = new AtomicLong(0);

  private final Map<Long, T> memoryMap;
  private final String name;
  private final LongAdder reads;
  private final LongAdder writes;
  private final LongAdder deletes;
  private final ExpiredStorableHandler expiredStorableHandler;
  private final ExpireStorableTaskManager<T> expireStorableTaskManager;
  @Getter
  private final TaskQueue taskScheduler;

  private long lastKeyStored;
  private long lastAccess;

  public MemoryStorage(ExpiredStorableHandler expiredStorableHandler, int expiredEventPoll) {
    memoryMap = new LinkedHashMap<>();
    this.expiredStorableHandler = Objects.requireNonNullElseGet(expiredStorableHandler, () -> new BaseExpiredHandler<>(this));
    taskScheduler = new TaskQueue();
    this.expireStorableTaskManager = new ExpireStorableTaskManager<>(this, taskScheduler, expiredEventPoll);
    name = "memory" + counter.get();
    reads = new LongAdder();
    writes = new LongAdder();
    deletes = new LongAdder();
    lastKeyStored = 0;
    lastAccess = System.currentTimeMillis();
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void close() throws IOException {
    while (taskScheduler.hasTasks()) {
      taskScheduler.executeTasks();
    }
    taskScheduler.abortAll();
    memoryMap.clear();
  }

  @Override
  public void delete() throws IOException {
    close();
  }

  @Override
  public void add(@NotNull T object) throws IOException {
    memoryMap.put(object.getKey(), object);
    writes.increment();
    expireStorableTaskManager.added(object);
    if (lastKeyStored < object.getKey()) {
      lastKeyStored = object.getKey();
    }
    lastAccess = System.currentTimeMillis();
  }

  @Override
  public boolean remove(long key) throws IOException {
    lastAccess = System.currentTimeMillis();
    boolean val = memoryMap.remove(key) != null;
    if(val) deletes.increment();
    return val;
  }

  @Override
  public T get(long key) throws IOException {
    lastAccess = System.currentTimeMillis();
    reads.increment();
    return memoryMap.get(key);
  }

  @Override
  public @NotNull List<Long> getKeys() {
    List<Long> keyList = new NaturalOrderedLongList();
    keyList.addAll(memoryMap.keySet());
    return keyList;
  }

  @Override
  public boolean contains(long key) {
    return memoryMap.containsKey(key);
  }

  public void scanForExpired() throws IOException {
    long now = System.currentTimeMillis();
    try (BitSetFactory bitSetFactory = new BitSetFactoryImpl(8192)) {
      Queue<Long> expired = new NaturalOrderedLongQueue(0, bitSetFactory);
      for (Map.Entry<Long, T> entry : memoryMap.entrySet()) {
        if (entry.getValue().getExpiry() != 0 && entry.getValue().getExpiry() < now) {
          expired.add(entry.getKey());
        }
      }
      if (!expired.isEmpty()) {
        expiredStorableHandler.expired(expired);
      }
    }
  }

  @Override
  public long size() throws IOException {
    lastAccess = System.currentTimeMillis();
    return memoryMap.size();
  }

  @Override
  public long getLastKey() {
    return lastKeyStored;
  }

  @Override
  public long getLastAccess() {
    return lastAccess;
  }

  @Override
  public boolean isEmpty() {
    return memoryMap.isEmpty();
  }

  @Override
  public @NotNull Collection<Long> keepOnly(@NotNull Collection<Long> listToKeep) {
    List<Long> itemsToRemove = new ArrayList<>(memoryMap.keySet());
    itemsToRemove.removeIf(listToKeep::contains);
    if (!itemsToRemove.isEmpty()) {
      for (long key : itemsToRemove) {
        memoryMap.remove(key);
      }
    }

    if (itemsToRemove.size() != listToKeep.size()) {
      Set<Long> actual = memoryMap.keySet();
      listToKeep.removeIf(actual::contains);
      return listToKeep;
    }
    return new ArrayList<>();
  }

  @Override
  public void setExecutor(TaskScheduler scheduler) {
    // The memory storage doesn't use the scheduler for any tasks
  }

  public @NotNull Statistics getStatistics() {
    return new StorageStatistics(reads.sumThenReset(), writes.sumThenReset(), deletes.sumThenReset());
  }

  @Override
  public boolean isCacheable() {
    return false;
  }

  @Override
  public int removeAll(@NotNull Collection<Long> listToRemove) {
    int count = 0;
    if (!listToRemove.isEmpty()) {
      for (long key : listToRemove) {
        if(memoryMap.remove(key) != null) {
          count++;
        }
      }
    }
    return count;
  }
}
