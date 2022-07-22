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

package io.mapsmessaging.storage.impl.cache.weak;

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.impl.cache.Cache;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.WeakHashMap;

public class WeakReferenceCacheStorage<T extends Storable> implements Cache<T> {

  private final Map<Long, T> weakMap;
  private final String name;

  public WeakReferenceCacheStorage() {
    weakMap = new LinkedHashMap<>();
    name = "WeakReference";
  }


  public WeakReferenceCacheStorage(String name) {
    weakMap = new WeakHashMap<>();
    this.name = name;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public T cacheGet(long key) {
    T obj = weakMap.get(key);
    if (obj != null && obj.getExpiry() != 0 && obj.getExpiry() < System.currentTimeMillis()) {
      obj = null;
      weakMap.remove(key);
    }
    return obj;
  }

  @Override
  public void cachePut(T obj) {
    weakMap.put(obj.getKey(), obj);
  }

  @Override
  public void cacheRemove(long key) {
    weakMap.remove(key);
  }

  @Override
  public void cacheClear() {
    weakMap.clear();
  }

  @Override
  public void cacheDelete() {
    cacheClear();
  }

  @Override
  public int size() {
    return weakMap.size();
  }
}
