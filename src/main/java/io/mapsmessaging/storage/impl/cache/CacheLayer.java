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
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.storage.tasks.Completion;
import io.mapsmessaging.utilities.threads.tasks.TaskScheduler;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class CacheLayer<T extends Storable> implements LayeredStorage<T>, Cache<T> {


  protected final Storage<T> baseStorage;
  private final boolean enableWriteThrough;
  protected final LongAdder cacheMiss = new LongAdder();
  protected final LongAdder cacheHit =new LongAdder();

  protected CacheLayer(){
    baseStorage = null;
    enableWriteThrough = false;
  }

  protected CacheLayer(boolean enableWriteThrough, Storage<T> baseStorage) {
    this.baseStorage = baseStorage;
    this.enableWriteThrough = enableWriteThrough;
  }

  public long getCacheMiss() {
    return cacheMiss.sum();
  }

  public long getCacheHit() {
    return cacheHit.sum();
  }

  @Override
  public String getName() {
    if(baseStorage != null)
      return baseStorage.getName();
    return "";
  }

  @Override
  public void delete() throws IOException {
    cacheDelete();
    if(baseStorage != null) baseStorage.delete();
  }


  @Override
  public void add(@NotNull T object,  Completion<T> completion) throws IOException{
    cachePut(object);
    if(enableWriteThrough && completion != null ){
      completion.onCompletion(object);
    }
    if(baseStorage != null) baseStorage.add(object);
  }

  @Override
  public void add(@NotNull T object) throws IOException {
    cachePut(object);
    if(baseStorage != null) baseStorage.add(object);
  }

  @Override
  public boolean remove(long key) throws IOException {
    cacheRemove(key);
    if(baseStorage != null)
      return baseStorage.remove(key);
    return false;
  }

  @Override
  public long size() throws IOException {
    if(baseStorage != null) return baseStorage.size();
    return 0;
  }

  @Override
  public boolean isEmpty() {
    if(baseStorage != null) return baseStorage.isEmpty();
    return false;
  }

  @Override
  public @NotNull List<Long> keepOnly(@NotNull List<Long> listToKeep) throws IOException {
    cacheClear();
    if(baseStorage != null)
      return baseStorage.keepOnly(listToKeep);
    return new ArrayList<>();
  }

  @Override
  public void close() throws IOException {
    cacheDelete();
    if(baseStorage != null) baseStorage.close();
  }

  @Override
  public @Nullable T get(long key) throws IOException {
    T obj = cacheGet(key);
    if (obj == null) {
      cacheMiss.increment();
      if(baseStorage != null) {
        obj = baseStorage.get(key);
      }
      if (obj != null) {
        cachePut(obj);
      }
    }
    else{
      cacheHit.increment();
    }
    return obj;
  }

  @Override
  public void setTaskQueue(TaskScheduler scheduler) {}

}
