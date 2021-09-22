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

package io.mapsmessaging.storage.impl.cache.weak;

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.storage.impl.cache.BaseLayeredStorage;
import io.mapsmessaging.storage.tasks.Completion;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class WeakReferenceCacheStorage<T extends Storable> extends BaseLayeredStorage<T> {

  private final Map<Long, T> cache;

  public WeakReferenceCacheStorage(){
    super(false, null);
    cache = new LinkedHashMap<>();
  }


  public WeakReferenceCacheStorage(boolean enableWriteThrough, Storage<T> actual) {
    super(enableWriteThrough, actual);
    cache = new WeakHashMap<>();
  }

  @Override
  public String getName(){
    return "WeakReference";
  }

  @Override
  public void delete() throws IOException {
    super.delete();
    cache.clear();
  }

  @Override
  public void add(@NotNull T object,  Completion<T> completion) throws IOException{
    cache.put(object.getKey(), object);
    super.add(object, completion);
  }

  @Override
  public void add(@NotNull T object) throws IOException {
    cache.put(object.getKey(), object);
    super.add(object);
  }

  @Override
  public boolean remove(long key) throws IOException {
    super.remove(key);
    cache.remove(key);
    return true;
  }

  @Override
  public @Nullable T get(long key) throws IOException {
    T obj = cache.get(key);
    if (obj == null) {
      obj = super.get(key);
      if (obj != null) {
        cache.put(key, obj);
      }
    }
    return obj;
  }

  @Override
  public @NotNull List<Long> keepOnly(@NotNull List<Long> listToKeep) throws IOException {
    cache.clear();
    return super.keepOnly(listToKeep);
  }

  @Override
  public void close() throws IOException {
    cache.clear();
    super.close();
  }
}
