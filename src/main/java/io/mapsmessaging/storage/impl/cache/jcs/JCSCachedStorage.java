/*
 *    Copyright [ 2020 - 2024 ] [Matthew Buckton]
 *    Copyright [ 2024 - 2025 ] [Maps Messaging B.V.]
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
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

package io.mapsmessaging.storage.impl.cache.jcs;

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.impl.cache.Cache;
import org.apache.commons.jcs3.JCS;
import org.apache.commons.jcs3.access.CacheAccess;

public class JCSCachedStorage<T extends Storable> implements Cache<T> {

  private final CacheAccess<Long, T> cache;

  public JCSCachedStorage() {
    cache = null;
  }

  public JCSCachedStorage(String name) {
    cache = JCS.getInstance(name + "_cache");
  }

  @Override
  public String getName() {
    return "JCS";
  }


  @Override
  public T cacheGet(long key) {
    T obj = cache.get(key);
    if (obj != null && obj.getExpiry() != 0 && obj.getExpiry() < System.currentTimeMillis()) {
      obj = null;
      cache.remove(key);
    }
    return obj;
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
  public void cacheClear() {
    cache.clear();
  }

  @Override
  public void cacheDelete() {
    cacheClear();
    cache.dispose();
  }

  @Override
  public int size() {
    return 0;
  }

  @Override
  public Cache<T> getInstance(String name) {
    return new JCSCachedStorage<>(name);
  }
}
