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

public class StorageFactoryFactory {

  private static final StorageFactoryFactory instance = new StorageFactoryFactory();
  private final ServiceLoader<StorageFactory> storageFactories;

  private StorageFactoryFactory() {
    storageFactories = ServiceLoader.load(StorageFactory.class);
  }

  public static StorageFactoryFactory getInstance() {
    return instance;
  }

  public List<String> getKnown() {
    List<String> known = new ArrayList<>();
    storageFactories.forEach(storageFactory -> known.add(storageFactory.getName()));
    return known;
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
        constructor.setAccessible(true);
        return (StorageFactory<T>) constructor.newInstance(properties, factory);
      }
    }
    return null;
  }

}
