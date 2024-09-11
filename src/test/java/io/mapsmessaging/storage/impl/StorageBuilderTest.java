/*
 *   Copyright [2020 - 2024]   [Matthew Buckton]
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

package io.mapsmessaging.storage.impl;

import io.mapsmessaging.storage.StorageBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;

class StorageBuilderTest {

  @BeforeAll
  static void initialise(){
    StorageBuilder.initialiseLayer();
  }

  @Test
  void testTypeSetters(){
    StorageBuilder<BaseTest.MappedData> storageBuilder = new StorageBuilder<>();
    try {
      storageBuilder.setStorageType("Memory");
    } catch (IOException e) {
      Assertions.fail("Should not throw exception", e.getCause());
    }
    Assertions.assertThrowsExactly(IOException.class, () -> storageBuilder.setStorageType("Memory"));
    StorageBuilder<BaseTest.MappedData> storageBuilder1= new StorageBuilder<>();
    Assertions.assertThrowsExactly(IOException.class, () -> storageBuilder1.setStorageType("error"));
  }

  @Test
  void testCacheSetters(){
    StorageBuilder<BaseTest.MappedData> storageBuilder = new StorageBuilder<>();
    try {
      storageBuilder.setCache();
    } catch (IOException e) {
      Assertions.fail("Should not throw exception", e.getCause());
    }
    Assertions.assertThrowsExactly(IOException.class, storageBuilder::setCache);
    StorageBuilder<BaseTest.MappedData> storageBuilder1= new StorageBuilder<>();
    Assertions.assertThrowsExactly(IOException.class, () -> storageBuilder1.setCache("error"));
  }

  @Test
  void generalSetters(){
    StorageBuilder<BaseTest.MappedData> storageBuilder = new StorageBuilder<>();
    storageBuilder.enableCacheWriteThrough(true);
    storageBuilder.setExpiredHandler(listOfExpiredEntries -> {

    });
    Assertions.assertTrue(storageBuilder.isEnableWriteThrough());
    Assertions.assertNotNull(storageBuilder.getExpiredStorableHandler());
  }
}
