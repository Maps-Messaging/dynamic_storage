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

package io.mapsmessaging.storage.impl.cache;

import io.mapsmessaging.storage.LayeredStorage;
import io.mapsmessaging.storage.Statistics;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.storage.tasks.Completion;
import io.mapsmessaging.utilities.threads.tasks.TaskScheduler;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;

public class CacheLayer<T extends Storable> implements LayeredStorage<T> {

  private final boolean enableWriteThrough;
  private final Cache<T> cache;
  private final Storage<T> baseStorage;

  private final LongAdder cacheMiss = new LongAdder();
  private final LongAdder cacheHit =new LongAdder();

  public CacheLayer(boolean enableWriteThrough, @NotNull Cache<T> cache, @NotNull Storage<T> baseStorage) {
    this.baseStorage = baseStorage;
    this.enableWriteThrough = enableWriteThrough;
    this.cache = cache;
  }

  @Override
  public void shutdown() throws IOException {
    baseStorage.shutdown();
  }

  @Override
  public String getName() {
      return baseStorage.getName();
  }

  @Override
  public void delete() throws IOException {
    cache.cacheDelete();
    baseStorage.delete();
  }

  @Override
  public void add(@NotNull T object,  Completion<T> completion) throws IOException{
    cache.cachePut(object);
    if(enableWriteThrough && completion != null ){
      completion.onCompletion(object);
    }
    baseStorage.add(object);
  }

  @Override
  public void add(@NotNull T object) throws IOException {
    cache.cachePut(object);
    baseStorage.add(object);
  }

  @Override
  public boolean remove(long key) throws IOException {
    cache.cacheRemove(key);
    return baseStorage.remove(key);
  }

  @Override
  public long size() throws IOException {
    return baseStorage.size();
  }

  @Override
  public boolean isEmpty() {
    return baseStorage.isEmpty();
  }

  @Override
  public @NotNull List<Long> keepOnly(@NotNull List<Long> listToKeep) throws IOException {
    cache.cacheClear();
    return baseStorage.keepOnly(listToKeep);
  }

  @Override
  public void close() throws IOException {
    cache.cacheDelete();
    baseStorage.close();
  }

  @Override
  public @Nullable T get(long key) throws IOException {
    T obj = cache.cacheGet(key);
    if (obj == null) {
      cacheMiss.increment();
      obj = baseStorage.get(key);
      if (obj != null) {
        cache.cachePut(obj);
      }
    }
    else{
      cacheHit.increment();
    }
    return obj;
  }

  @Override
  public void setExecutor(TaskScheduler scheduler) {
    baseStorage.setExecutor(scheduler);
  }

  public Statistics getStatistics(){
    return new CacheStatistics(cacheMiss.sumThenReset(), cacheHit.sumThenReset(), cache.size(), baseStorage.getStatistics());
  }
}
