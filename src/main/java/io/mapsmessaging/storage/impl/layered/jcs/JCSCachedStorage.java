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

package io.mapsmessaging.storage.impl.layered.jcs;

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.storage.impl.layered.BaseLayeredStorage;
import java.io.IOException;
import java.util.List;
import org.apache.commons.jcs3.JCS;
import org.apache.commons.jcs3.access.CacheAccess;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class JCSCachedStorage<T extends Storable> extends BaseLayeredStorage<T> {

  private final CacheAccess<Long, T> cache;


  public JCSCachedStorage(Storage<T> baseStorage) {
    super(baseStorage);
    cache = JCS.getInstance(baseStorage.getName() + "_cache");
  }


  @Override
  public @NotNull List<Long> keepOnly(@NotNull List<Long> listToKeep) throws IOException {
    cache.clear();
    return super.keepOnly(listToKeep);
  }

  @Override
  public void close() throws IOException {
    cache.clear();
    cache.dispose();
    super.close();
  }

  @Override
  public void delete() throws IOException {
    cache.clear();
    cache.dispose();
    super.delete();
  }

  @Override
  public void add(@NotNull T object) throws IOException {
    super.add(object);
    cache.put(object.getKey(), object);
  }

  @Override
  public boolean remove(long key) throws IOException {
    cache.remove(key);
    return super.baseStorage.remove(key);
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

}