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

package io.mapsmessaging.storage.impl.cache.jcs;

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.storage.impl.cache.CacheLayer;
import io.mapsmessaging.storage.tasks.Completion;
import io.mapsmessaging.utilities.threads.tasks.TaskScheduler;
import java.io.IOException;
import java.util.List;
import org.apache.commons.jcs3.JCS;
import org.apache.commons.jcs3.access.CacheAccess;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class JCSCachedStorage<T extends Storable> extends CacheLayer<T> {

  private final CacheAccess<Long, T> cache;

  public JCSCachedStorage(){
    super(false,null);
    cache = null;
  }

  public JCSCachedStorage(boolean enableWriteThrough, Storage<T> baseStorage) {
    super(enableWriteThrough, baseStorage);
    cache = JCS.getInstance(baseStorage.getName() + "_cache");
  }

  @Override
  public String getName(){
    return "JCS";
  }

  @Override
  public void close() throws IOException {
    super.close();
    cache.dispose();
  }

  @Override
  public void delete() throws IOException {
    super.delete();
    cache.dispose();
  }

  @Override
  protected T cacheGet(long key) {
    return cache.get(key);
  }

  @Override
  protected void cachePut(T obj) {
    cache.put(obj.getKey(), obj);
  }

  @Override
  protected void cacheRemove(long key) {
    cache.remove(key);
  }

  protected void cacheClear(){
    cache.clear();
  }
}
