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

  private static final StorageFactoryFactory instance = new StorageFactoryFactory();
  private final ServiceLoader<StorageFactory> storageFactories;

  private final ServiceLoader<LayeredStorage> layeredStorages;


  private StorageFactoryFactory() {
    storageFactories = ServiceLoader.load(StorageFactory.class);
    layeredStorages = ServiceLoader.load(LayeredStorage.class);
  }

  public static StorageFactoryFactory getInstance() {
    return instance;
  }

  public List<String> getKnownStorages() {
    List<String> known = new ArrayList<>();
    storageFactories.forEach(storageFactory -> known.add(storageFactory.getName()));
    return known;
  }

  public List<String> getKnownLayers() {
    List<String> layered = new ArrayList<>();
    layeredStorages.forEach(layer -> layered.add(layer.getName()));
    return layered;
  }

  @SneakyThrows
  @Nullable
  public <T extends Storable> StorageFactory<T> create(@NotNull String name, @NotNull Map<String, String> properties, @NotNull Factory<T> factory) {
    Optional<Provider<StorageFactory>> first = storageFactories.stream().filter(storageFactoryProvider -> storageFactoryProvider.get().getName().equals(name)).findFirst();
    if (first.isPresent()) {
      StorageFactory<?> found = first.get().get();
      Class<T> clazz = (Class<T>) found.getClass();
      Constructor<T>[] constructors = (Constructor<T>[]) clazz.getDeclaredConstructors();
      Constructor<T> constructor = null;
      for (Constructor<T> cstr : constructors) {
        if (cstr.getParameters().length == 2) {
          constructor = cstr;
          break;
        }
      }
      if (constructor != null) {
        return (StorageFactory<T>) constructor.newInstance(properties, factory);
      }
    }
    return null;
  }

  @SneakyThrows
  @NotNull
  public <T extends Storable> LayeredStorage<T> createLayer(@NotNull String name, boolean enableWriteThrough, @NotNull Storage<T> baseStore) {
    Optional<Provider<LayeredStorage>> first = layeredStorages.stream().filter(layer -> layer.get().getName().equals(name)).findFirst();
    if (first.isPresent()) {
      LayeredStorage<?> found = first.get().get();
      Class<T> clazz = (Class<T>) found.getClass();
      Constructor<T>[] constructors = (Constructor<T>[]) clazz.getDeclaredConstructors();
      Constructor<T> constructor = null;
      for (Constructor<T> cstr : constructors) {
        if (cstr.getParameters().length == 2) {
          constructor = cstr;
          break;
        }
      }
      if (constructor != null) {
        return (LayeredStorage<T>) constructor.newInstance(enableWriteThrough, baseStore);
      }
    }
    throw new IOException("Unknown layered storage declared");
  }


}
