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
import io.mapsmessaging.storage.impl.cache.Cache;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.WeakHashMap;

public class WeakReferenceCacheStorage<T extends Storable> implements Cache<T> {

  private final Map<Long, T> cache;

  public WeakReferenceCacheStorage(){
    cache = new LinkedHashMap<>();
  }


  public WeakReferenceCacheStorage(String name) {
    cache = new WeakHashMap<>();
  }

  @Override
  public String getName(){
    return "WeakReference";
  }

  @Override
  public T cacheGet(long key) {
    return cache.get(key);
  }

  @Override
  public void cachePut(T obj) {
    cache.put(obj.getKey(), obj);
  }

  @Override
  public void cacheRemove(long key) {
    cache.remove(key);
  }

  @Override
  public void cacheClear(){
    cache.clear();
  }

  @Override
  public void cacheDelete(){
    cacheClear();
  }
}
