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
import java.io.IOException;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class BaseLayeredStorage<T extends Storable> implements LayeredStorage<T> {


  protected final Storage<T> baseStorage;
  private final boolean enableWriteThrough;

  protected BaseLayeredStorage(boolean enableWriteThrough, Storage<T> baseStorage) {
    this.baseStorage = baseStorage;
    this.enableWriteThrough = enableWriteThrough;
  }

  @Override
  public String getName() {
    return baseStorage.getName();
  }

  @Override
  public void delete() throws IOException {
    baseStorage.delete();
  }

  public void add(@NotNull T object,  Completion<T> completion) throws IOException{
    if(enableWriteThrough && completion != null ){
      completion.onCompletion(object);
    }
    baseStorage.add(object);
  }


  @Override
  public void add(@NotNull T object) throws IOException {
    baseStorage.add(object);
  }

  @Override
  public boolean remove(long key) throws IOException {
    return baseStorage.remove(key);
  }

  @Override
  public @Nullable T get(long key) throws IOException {
    return baseStorage.get(key);
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
    return baseStorage.keepOnly(listToKeep);
  }

  @Override
  public void close() throws IOException {
    baseStorage.close();
  }

  @Override
  public boolean isCacheable() {
    return false;
  }

}
