/*
 *
 *  Copyright [ 2020 - 2024 ] Matthew Buckton
 *  Copyright [ 2024 - 2025 ] MapsMessaging B.V.
 *
 *  Licensed under the Apache License, Version 2.0 with the Commons Clause
 *  (the "License"); you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at:
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *      https://commonsclause.com/
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.mapsmessaging.storage.impl;

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.StorableFactory;
import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.storage.StorageBuilder;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SimpleStorableFactoryCreationTest {


  @Test
  public void createInstances() throws IOException {

    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("Sync", "false");
    properties.put("basePath", "./test.db");
    List<String> known = StorageBuilder.getKnownStorages();
    Assertions.assertFalse(known.isEmpty());
    for (String test : known) {
      StorageBuilder<StorableString> storageBuilder = new StorageBuilder<>();
      storageBuilder.setStorageType(test)
          .setFactory(new SimpleStorableFactory())
          .setName("Test")
          .setProperties(properties);
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
    List<String> known = StorageBuilder.getKnownStorages();
    Assertions.assertFalse(known.isEmpty());
    for (String layer : caches) {
      for (String test : known) {
        StorageBuilder<StorableString> storageBuilder = new StorageBuilder<>();
        storageBuilder.setStorageType(test)
            .setFactory(new SimpleStorableFactory())
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
    List<String> known = StorageBuilder.getKnownStorages();
    Assertions.assertFalse(known.isEmpty());
    for (String test : known) {
      StorageBuilder<StorableString> storageBuilder = new StorageBuilder<>();
      storageBuilder.setStorageType(test)
          .setFactory(new SimpleStorableFactory())
          .setName("Test")
          .setCache()
          .setProperties(properties);
      Storage<StorableString> store = storageBuilder.build();
      Assertions.assertNotNull(store);
      store.delete();
    }
  }


  static final class SimpleStorableFactory implements StorableFactory<StorableString> {

    @Override
    public @NotNull StorableString unpack(@NotNull ByteBuffer[] buffers) {
      return new StorableString();
    }

    @Override
    public @NotNull ByteBuffer[] pack(@NotNull StorableString object) throws IOException {
      return object.write();
    }
  }

  static final class StorableString implements Storable {

    @Override
    public long getKey() {
      return 0;
    }

    @Override
    public long getExpiry() {
      return 0;
    }

    public @NotNull ByteBuffer[] write() {
      return new ByteBuffer[0];
    }
  }

}
