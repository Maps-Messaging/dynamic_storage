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

package io.mapsmessaging.storage;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class StorageBuilder<T extends Storable> {

  private String storeType;
  private String cacheName;
  private String name;
  private Map<String, String> properties;
  private StorableFactory<T> storableFactory;
  private ExpiredStorableHandler<T> expiredStorableHandler;

  private boolean enableWriteThrough = false;

  public @NotNull StorageBuilder<T> setName(@NotNull String name) {
    this.name = name;
    return this;
  }

  public @NotNull StorageBuilder<T> setExpiredHandler(@NotNull ExpiredStorableHandler<T> expiredStorableHandler) {
    this.expiredStorableHandler = expiredStorableHandler;
    return this;
  }

  public @NotNull StorageBuilder<T> setProperties(@NotNull Map<String, String> properties) {
    this.properties = properties;
    return this;
  }

  public @NotNull StorageBuilder<T> setFactory(@NotNull StorableFactory<T> storableFactory) {
    this.storableFactory = storableFactory;
    return this;
  }

  public @NotNull StorageBuilder<T> enableCacheWriteThrough(boolean enableWriteThrough) {
    this.enableWriteThrough = enableWriteThrough;
    return this;
  }

  public @NotNull StorageBuilder<T> setStorageType(@NotNull String storeType) throws IOException {
    if(this.storeType != null){
      throw new IOException("Store type already defined");
    }
    List<String> known = StorageFactoryFactory.getInstance().getKnownStorages();
    for(String type:known){
      if(storeType.equals(type)){
        this.storeType = type;
        break;
      }
    }
    if(this.storeType == null){
      throw new IOException("No known storage type defined "+storeType);
    }
    return this;
  }

  public @NotNull StorageBuilder<T> setCache() throws IOException {
    return setCache(null);
  }

  public  @NotNull StorageBuilder<T> setCache(@Nullable String cacheName) throws IOException {
    if(this.cacheName != null){
      throw new IOException("Cache already specified");
    }
    if(cacheName == null){
      this.cacheName = "WeakReference";
    }
    else{
      List<String> layered = StorageFactoryFactory.getInstance().getKnownLayers();
      for(String layer:layered){
        if(cacheName.equals(layer)){
          this.cacheName = cacheName;
          break;
        }
      }
    }
    if( this.cacheName == null){
      throw new IOException("No such cache implementation found "+cacheName);
    }
    return this;
  }


  public Storage<T> build() throws IOException {
    StorageFactory<T> storeFactory = StorageFactoryFactory.getInstance().create(storeType, properties, storableFactory, expiredStorableHandler);
    if(storeFactory != null) {
      Storage<T> baseStore = storeFactory.create(name);
      if (baseStore.isCacheable() && cacheName != null) {
        baseStore = StorageFactoryFactory.getInstance().createCache(cacheName, enableWriteThrough, baseStore);
      }
      return baseStore;
    }
    else{
      throw new IOException("Unable to construct new store");
    }
  }

  public static List<String> getKnownStorages(){
    return StorageFactoryFactory.getInstance().getKnownStorages();
  }


  public static List<String> getKnownLayers(){
    return StorageFactoryFactory.getInstance().getKnownLayers();
  }

}
