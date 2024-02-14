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

package io.mapsmessaging.storage;

import io.mapsmessaging.logging.Logger;
import io.mapsmessaging.logging.LoggerFactory;
import io.mapsmessaging.storage.impl.cache.Cache;
import io.mapsmessaging.storage.impl.cache.CacheLayer;
import io.mapsmessaging.storage.logging.StorageLogMessages;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.*;

@SuppressWarnings({"java:S6548", "java:S3740"}) // yes it is a singleton
class StorageFactoryFactory {

  private static class Holder {
    static final StorageFactoryFactory INSTANCE = new StorageFactoryFactory();
  }

  public static StorageFactoryFactory getInstance() {
    return Holder.INSTANCE;
  }

  private static final Logger logger = LoggerFactory.getLogger(StorageFactoryFactory.class);

  private final List<StorageFactory> storageFactories;
  private final List<Cache> caches;
  private final List<String> layered = new ArrayList<>();
  private final List<String> known = new ArrayList<>();

  private StorageFactoryFactory() {
    ServiceLoader<StorageFactory> serviceLoader = ServiceLoader.load(StorageFactory.class);
    ServiceLoader<Cache> serviceCaches = ServiceLoader.load(Cache.class);

    storageFactories = new ArrayList<>();
    for(StorageFactory storageFactory:serviceLoader){
      storageFactories.add(storageFactory);
    }

    caches = new ArrayList<>();
    for(Cache cache:serviceCaches){
      caches.add(cache);
    }

    caches.forEach(layer -> layered.add(layer.getName()));
    storageFactories.forEach(storageFactory -> known.add(storageFactory.getName()));
  }

  public List<String> getKnownStorages() {
    return known;
  }

  public List<String> getKnownLayers(){
    return layered;
  }

  @SuppressWarnings("java:S2293")
  @SneakyThrows
  @Nullable <T extends Storable> StorageFactory<T> create(@NotNull String name, @NotNull Map<String, String> properties, @NotNull StorableFactory<T> storableFactory, ExpiredStorableHandler expiredStorableHandler) {
    Optional<StorageFactory> first = storageFactories.stream().filter(storageFactoryProvider -> storageFactoryProvider.getName().equals(name)).findFirst();
    if (first.isPresent()) {
      logger.log(StorageLogMessages.FOUND_FACTORY, first.get().getClass().getName());
      StorageFactory<T> found = first.get();
      return found.getInstance(properties, storableFactory, expiredStorableHandler);
    }
    else {
      logger.log(StorageLogMessages.NO_MATCHING_FACTORY, name);
    }

    return null;
  }

  @SuppressWarnings("java:S2293")
  @SneakyThrows
  @NotNull <T extends Storable> CacheLayer<T> createCache(@NotNull String name, boolean enableWriteThrough, @NotNull Storage<T> baseStore) {
    Optional<Cache> first = caches.stream().filter(layer -> layer.getName().equals(name)).findFirst();
    if (first.isPresent()) {
      logger.log(StorageLogMessages.FOUND_CACHE_FACTORY, name);
      Cache<?> found = first.get();
      Class<T> clazz = (Class<T>) found.getClass();
      Constructor<T>[] constructors = (Constructor<T>[]) clazz.getDeclaredConstructors();
      Constructor<T> constructor = null;
      for (Constructor<T> cstr : constructors) {
        if (cstr.getParameters().length == 1) {
          logger.log(StorageLogMessages.FOUND_CACHE_CONSTRUCTOR, name);
          constructor = cstr;
          break;
        }
      }
      if (constructor != null) {
        Cache<T> cache = (Cache<T>) constructor.newInstance(baseStore.getName());
        logger.log(StorageLogMessages.CREATED_NEW_CACHE_INSTANCE, name);
        return new CacheLayer<T>(enableWriteThrough, cache, baseStore);
      }
    }
    logger.log(StorageLogMessages.NO_CACHE_FOUND, name);

    throw new IOException("Unknown layered storage declared");
  }


}
