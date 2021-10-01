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

package io.mapsmessaging.storage.impl.memory;

import io.mapsmessaging.storage.Statistics;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.storage.StorageStatistics;
import io.mapsmessaging.utilities.threads.tasks.TaskScheduler;
import java.util.concurrent.atomic.LongAdder;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class MemoryStorage<T extends Storable> implements Storage<T> {

  private static final AtomicLong counter = new AtomicLong(0);

  private final Map<Long, T> memoryMap;
  private final String name;
  private final LongAdder reads;
  private final LongAdder writes;
  private final LongAdder deletes;

  public MemoryStorage() {
    memoryMap = new LinkedHashMap<>();
    name = "memory" + counter.get();
    reads = new LongAdder();
    writes = new LongAdder();
    deletes = new LongAdder();
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void delete() throws IOException {
    memoryMap.clear();
  }

  @Override
  public void add(@NotNull T object) throws IOException {
    memoryMap.put(object.getKey(), object);
    writes.increment();
  }

  @Override
  public boolean remove(long key) throws IOException {
    deletes.increment();
    return memoryMap.remove(key) != null;
  }

  @Override
  public T get(long key) throws IOException {
    reads.increment();
    return memoryMap.get(key);
  }

  @Override
  public long size() throws IOException {
    return memoryMap.size();
  }

  @Override
  public boolean isEmpty() {
    return memoryMap.isEmpty();
  }

  @Override
  public @NotNull List<Long> keepOnly(@NotNull List<Long> listToKeep) {
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

  public Statistics getStatistics(){
    return new StorageStatistics(reads.sumThenReset(), writes.sumThenReset(), deletes.sumThenReset(), 0L, 0L,0L, 0L);
  }
  @Override
  public void close() throws IOException {
    memoryMap.clear();
  }

  @Override
  public boolean isCacheable() {
    return false;
  }

}
