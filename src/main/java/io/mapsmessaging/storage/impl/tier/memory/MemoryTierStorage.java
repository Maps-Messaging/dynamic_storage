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

import io.mapsmessaging.storage.Statistics;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.storage.impl.file.TaskQueue;
import io.mapsmessaging.storage.impl.tier.memory.tasks.TierMigrationTask;
import io.mapsmessaging.utilities.collections.NaturalOrderedLongList;
import io.mapsmessaging.utilities.collections.NaturalOrderedLongQueue;
import io.mapsmessaging.utilities.threads.tasks.TaskScheduler;
import lombok.Getter;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

public class MemoryTierStorage<T extends Storable> implements Storage<T> {

  private final Storage<ObjectMonitor<T>> primary;
  private final Storage<T> secondary;
  private final long memorySize;
  private final Queue<Long> memoryList;
  private final LongAdder migratedEvents;

  private ScheduledFuture<?> scanner;

  @Getter
  private final long scanInterval;

  @Getter
  private final long migrationTime;


  private long lastKey;

  public MemoryTierStorage(Storage<ObjectMonitor<T>> primary, Storage<T> secondary, long scanInterval, long migrationTime, long memorySize) {
    this.primary = primary;
    this.secondary = secondary;
    this.memorySize = memorySize;
    this.migrationTime =migrationTime;
    this.scanInterval = scanInterval;

    memoryList = new NaturalOrderedLongQueue();
    lastKey = -2;
    migratedEvents = new LongAdder();
  }

  @Override
  public void close() throws IOException {
    if (scanner != null) {
      scanner.cancel(false);
    }
    for (Long key : primary.getKeys()) {
      ObjectMonitor<T> monitor = primary.get(key);
      if (monitor != null) {
        secondary.add(monitor.getStorable());
      }
    }
    primary.close();
    secondary.close();
  }

  @Override
  public void delete() throws IOException {
    primary.delete();
    secondary.delete();
  }

  @Override
  public String getName() {
    return primary.getName();
  }

  @Override
  public long size() throws IOException {
    return primary.size() + secondary.size();
  }

  @Override
  public long getLastKey() {
    if(lastKey == -2){
      lastKey = secondary.getLastKey();
    }
    return lastKey;
  }

  @Override
  public long getLastAccess() {
    return primary.getLastAccess();
  }

  @Override
  public boolean isEmpty() {
    return primary.isEmpty() && secondary.isEmpty();
  }

  @Override
  public TaskQueue getTaskScheduler() {
    return primary.getTaskScheduler();
  }

  @Override
  public @NotNull Collection<Long> keepOnly(@NotNull Collection<Long> listToKeep) throws IOException {
    Collection<Long> next = primary.keepOnly(listToKeep);
    secondary.keepOnly(next);
    return next;
  }

  @Override
  public int removeAll(@NotNull Collection<Long> listToRemove) throws IOException {
    int count  = primary.removeAll(listToRemove);
    count += secondary.removeAll(listToRemove);
    return count;
  }

  @Override
  public @NotNull Statistics getStatistics() {
    return new MemoryTierStatistics(primary.getStatistics(), secondary.getStatistics(), migratedEvents.sumThenReset());
  }

  @Override
  public void add(@NotNull T object) throws IOException {
    lastKey = object.getKey();
    primary.add(new ObjectMonitor<>(object));

    // Push from primary to secondary once the limit has been reached
    if(memorySize > 0){
      memoryList.offer(object.getKey());
      while(memoryList.size() > memorySize) {
        Long key = memoryList.poll();
        if (key != null && key != -1) {
          ObjectMonitor<T> oldest = primary.get(key);
          if (oldest != null) {
            secondary.add(oldest.getStorable());
            primary.remove(key);
          }
        }
      }
    }
  }

  @Override
  public boolean remove(long key) throws IOException {
    memoryList.remove(key);
    return primary.remove(key) || secondary.remove(key);

  }

  @Override
  public @Nullable T get(long key) throws IOException {
    ObjectMonitor<T> monitor = primary.get(key);
    if (monitor != null) {
      monitor.setLastAccess(System.currentTimeMillis());
      return monitor.getStorable();
    }
    return secondary.get(key);
  }

  @Override
  public @NotNull List<Long> getKeys() {
    List<Long> keyList = new NaturalOrderedLongList();
    keyList.addAll(primary.getKeys());
    keyList.addAll(secondary.getKeys());
    return keyList;
  }

  @Override
  public boolean contains(long key) {
    return primary.contains(key) || secondary.contains(key);
  }

  @Override
  public void setExecutor(TaskScheduler executor) {
    secondary.setExecutor(executor);
    scanner = secondary.getTaskScheduler().scheduleAtFixedRate(new TierMigrationTask(this), scanInterval, TimeUnit.MILLISECONDS);
  }

  @SneakyThrows
  public void scan() {
    long test = System.currentTimeMillis() + migrationTime;
    for (Long key : primary.getKeys()) {
      ObjectMonitor<T> check = primary.get(key);
      if (check != null && check.getLastAccess() < test) {
        secondary.add(check.getStorable());
        primary.remove(key);
        memoryList.remove(key);
        migratedEvents.increment();
      }
    }
  }
}
