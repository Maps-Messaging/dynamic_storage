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

import io.mapsmessaging.logging.Logger;
import io.mapsmessaging.logging.LoggerFactory;
import io.mapsmessaging.storage.impl.cache.Cache;
import io.mapsmessaging.storage.impl.cache.CacheLayer;
import io.mapsmessaging.storage.logging.StorageLogMessages;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@SuppressWarnings("java:S3740") // This is not how ServiceLoaders work, we can not get a generic load
class StorageFactoryFactory {

  private static final Logger logger = LoggerFactory.getLogger(StorageFactoryFactory.class);

  private static final StorageFactoryFactory instance = new StorageFactoryFactory();
  private final ServiceLoader<StorageFactory> storageFactories;
  private final ServiceLoader<Cache> caches;
  private final List<String> layered = new ArrayList<>();
  private final List<String> known = new ArrayList<>();

  private StorageFactoryFactory() {
    storageFactories = ServiceLoader.load(StorageFactory.class);
    caches = ServiceLoader.load(Cache.class);
    caches.forEach(layer -> layered.add(layer.getName()));
    storageFactories.forEach(storageFactory -> known.add(storageFactory.getName()));
  }

  public static StorageFactoryFactory getInstance() {
    return instance;
  }

  public List<String> getKnownStorages() {
    return known;
  }

  public List<String> getKnownLayers(){
    return layered;
  }

  @SuppressWarnings("java:S2293")
  @SneakyThrows
  @Nullable <T extends Storable> StorageFactory<T> create(@NotNull String name, @NotNull Map<String, String> properties, @NotNull StorableFactory<T> storableFactory,
      ExpiredStorableHandler expiredStorableHandler) {
    Optional<Provider<StorageFactory>> first = storageFactories.stream().filter(storageFactoryProvider -> storageFactoryProvider.get().getName().equals(name)).findFirst();
    if (first.isPresent()) {
      logger.log(StorageLogMessages.FOUND_FACTORY, first.get().getClass().getName());
      StorageFactory<?> found = first.get().get();
      Class<T> clazz = (Class<T>) found.getClass();
      Constructor<T>[] constructors = (Constructor<T>[]) clazz.getDeclaredConstructors();
      Constructor<T> constructor = null;
      for (Constructor<T> cstr : constructors) {
        if (cstr.getParameters().length == 3) {
          constructor = cstr;
          logger.log(StorageLogMessages.FOUND_CONSTRUCTOR, name);
          break;
        }
      }
      if (constructor != null) {
        return (StorageFactory<T>) constructor.newInstance(properties, storableFactory, expiredStorableHandler);
      }
      else{
        logger.log(StorageLogMessages.NO_CONSTRUCTOR_FOUND, name);

      }
    }
    else {
      logger.log(StorageLogMessages.NO_MATCHING_FACTORY, name);
    }

    return null;
  }

  @SuppressWarnings("java:S2293")
  @SneakyThrows
  @NotNull <T extends Storable> CacheLayer<T> createCache(@NotNull String name, boolean enableWriteThrough, @NotNull Storage<T> baseStore) {
    Optional<Provider<Cache>> first = caches.stream().filter(layer -> layer.get().getName().equals(name)).findFirst();
    if (first.isPresent()) {
      logger.log(StorageLogMessages.FOUND_CACHE_FACTORY, name);
      Cache<?> found = first.get().get();
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
