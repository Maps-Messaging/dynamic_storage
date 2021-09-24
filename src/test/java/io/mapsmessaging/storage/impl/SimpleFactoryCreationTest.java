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

package io.mapsmessaging.storage.impl;

import io.mapsmessaging.storage.Factory;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.storage.StorageBuilder;
import io.mapsmessaging.storage.impl.streams.ObjectReader;
import io.mapsmessaging.storage.impl.streams.ObjectWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SimpleFactoryCreationTest {


  @Test
  public void createInstances() throws IOException {

    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("Sync", "false");
    properties.put("basePath", "./test.db");
    List<String> known =StorageBuilder.getKnownStorages();
    Assertions.assertFalse(known.isEmpty());
    for(String test:known) {
      StorageBuilder<StorableString> storageBuilder = new StorageBuilder<>();
      storageBuilder.setStorageType(test)
          .setFactory(new SimpleFactory())
          .setName("Test")
          .setProperties( properties);
      Storage<StorableString> store = storageBuilder.build();
      Assertions.assertNotNull(store);
      store.delete();
    }
  }

  @Test
  public void createCacheInstances() throws IOException {
    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("Sync", "false");
    properties.put("basePath", "./test.db");
    List<String> caches = StorageBuilder.getKnownLayers();
    List<String> known =StorageBuilder.getKnownStorages();
    Assertions.assertFalse(known.isEmpty());
    for(String layer:caches) {
      for (String test : known) {
        StorageBuilder<StorableString> storageBuilder = new StorageBuilder<>();
        storageBuilder.setStorageType(test)
            .setFactory(new SimpleFactory())
            .setName("Test")
            .setCache(layer)
            .setProperties(properties);
        Storage<StorableString> store = storageBuilder.build();
        Assertions.assertNotNull(store);
        store.delete();
      }
    }
  }

  @Test
  public void createCacheDefaultInstances() throws IOException {
    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("Sync", "false");
    properties.put("basePath", "./test.db");
    List<String> caches = StorageBuilder.getKnownLayers();
    List<String> known =StorageBuilder.getKnownStorages();
    Assertions.assertFalse(known.isEmpty());
    for (String test : known) {
      StorageBuilder<StorableString> storageBuilder = new StorageBuilder<>();
      storageBuilder.setStorageType(test)
          .setFactory(new SimpleFactory())
          .setName("Test")
          .setCache()
          .setProperties(properties);
      Storage<StorableString> store = storageBuilder.build();
      Assertions.assertNotNull(store);
      store.delete();
    }
  }


  static final class SimpleFactory implements Factory<StorableString> {

    @Override
    public StorableString create() {
      return new StorableString();
    }
  }

  static final class StorableString implements Storable {

    @Override
    public long getKey() {
      return 0;
    }

    @Override
    public void read(@NotNull ByteBuffer[] buffers) throws IOException {

    }

    @Override
    public @NotNull ByteBuffer[] write() throws IOException {
      return new ByteBuffer[0];
    }

  }

}
