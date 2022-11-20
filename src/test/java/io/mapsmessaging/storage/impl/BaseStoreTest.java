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

package io.mapsmessaging.storage.impl;

import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.utilities.threads.tasks.ThreadLocalContext;
import io.mapsmessaging.utilities.threads.tasks.ThreadStateContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;


public abstract class BaseStoreTest extends BaseTest {

  public abstract Storage<MappedData> createStore(String testName, boolean sync) throws IOException;

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void basicIOTestWithAndWithoutSync(boolean sync) throws IOException {
    Storage<MappedData> storage = null;
    try {
      storage = createStore(testName, sync);
      ThreadStateContext context = new ThreadStateContext();
      context.add("domain", "ResourceAccessKey");
      ThreadLocalContext.set(context);
      // Remove any before we start

      for (int x = 0; x < 10; x++) {
        storage.add(createMessageBuilder(x));
      }
      Assertions.assertEquals(10, storage.size());

      for (int x = 0; x < 10; x++) {
        MappedData message = storage.get(x);
        validateMessage(message, x);
        storage.remove(x);
        Assertions.assertNotNull(message);
      }
      Assertions.assertTrue(storage.isEmpty());
    } finally {
      if (storage != null) {
        storage.delete();
      }
    }
  }

  @Test
  void testTrailingDeletion() throws IOException {
    int eventCount = 10_000;
    int skipCount = 100;
    Storage<MappedData> storage = null;
    try {
      storage = createStore(testName, false);
      ThreadStateContext context = new ThreadStateContext();
      context.add("domain", "ResourceAccessKey");
      ThreadLocalContext.set(context);
      // Remove any before we start

      int deleteIndex = 0;
      for (int x = 0; x < eventCount; x++) {
        MappedData message = createMessageBuilder(x);
        validateMessage(message, x);
        storage.add(message);
        if(storage.size() > skipCount){
          Assertions.assertTrue(storage.remove(deleteIndex), "Failed to delete index "+deleteIndex);
          deleteIndex++;
          if(deleteIndex % skipCount == 0){
            deleteIndex++; // skip every 500
          }
        }
      }
      while(deleteIndex < eventCount) {
        Assertions.assertTrue(storage.remove(deleteIndex), "Failed to delete index " + deleteIndex);
        deleteIndex++;
        if (deleteIndex % skipCount == 0) {
          deleteIndex++; // skip every 500
        }
      }

      Assertions.assertEquals(skipCount-1, storage.size());

      for(int x=skipCount;x<eventCount;x= x+skipCount){
        Assertions.assertTrue(storage.contains(x), "Should contain index: "+x);
      }
      long index=skipCount;
      List<Long> keyList = storage.getKeys();
      for(Long key: keyList){
        Assertions.assertEquals(index, key);
        index += skipCount;
      }
      storage.keepOnly(new ArrayList<>());

      Assertions.assertTrue(storage.isEmpty());
    } finally {
      if (storage != null) {
        storage.delete();
      }
    }
  }

  @Test
  void basicIndexTests() throws IOException {
    Storage<MappedData> storage = null;
    try {
      storage = createStore(testName, false);
      ThreadStateContext context = new ThreadStateContext();
      context.add("domain", "ResourceAccessKey");
      ThreadLocalContext.set(context);
      // Remove any before we start

      for (int x = 0; x < 1000; x++) {
        MappedData message = createMessageBuilder(x);
        validateMessage(message, x);
        storage.add(message);
      }
      Assertions.assertEquals(1000, storage.size());

      for(int x=0;x<1000;x++){
        Assertions.assertTrue(storage.contains(x));
      }
      long index=0;
      for(Long key:storage.getKeys()){
        Assertions.assertEquals(index, key);
        index++;
      }
      storage.keepOnly(new ArrayList<>());

      Assertions.assertTrue(storage.isEmpty());
    } finally {
      if (storage != null) {
        storage.delete();
      }
    }
  }

  @Test
  void basicKeepOnlyTest() throws IOException {
    Storage<MappedData> storage = null;
    try {
      storage = createStore(testName, false);
      ThreadStateContext context = new ThreadStateContext();
      context.add("domain", "ResourceAccessKey");
      ThreadLocalContext.set(context);
      // Remove any before we start

      for (int x = 0; x < 1000; x++) {
        MappedData message = createMessageBuilder(x);
        validateMessage(message, x);
        storage.add(message);
      }
      Assertions.assertEquals(1000, storage.size());

      List<Long> keepList = new ArrayList<>();
      for (long x = 0; x < 1000; x = x + 2) {
        keepList.add(x);
      }
      storage.keepOnly(keepList);
      Assertions.assertEquals(500, storage.size());

      for (int x = 0; x < 1000; x++) {
        MappedData message = storage.get(x);
        if (x % 2 == 0) {
          validateMessage(message, x);
          storage.remove(x);
          Assertions.assertNotNull(message);
        } else {
          Assertions.assertNull(message);
        }
      }
      Assertions.assertTrue(storage.isEmpty());
    } finally {
      if (storage != null) {
        storage.delete();
      }
    }
  }

  @Test
  void basicPauseResume() throws IOException {
    Storage<MappedData> storage = null;
    try {
      storage = createStore(testName, false);
      if (storage.supportPause()) {
        ThreadStateContext context = new ThreadStateContext();
        context.add("domain", "ResourceAccessKey");
        ThreadLocalContext.set(context);
        for (int x = 0; x < 1000; x++) {
          storage.add(createMessageBuilder(x));
        }
        Assertions.assertEquals(1000, storage.size());
        storage.pause();
        Assertions.assertEquals(1000, storage.size());
        storage.resume();
        Assertions.assertEquals(1000, storage.size());
        for (int x = 0; x < 1000; x++) {
          MappedData message = storage.get(x);
          validateMessage(message, x);
          storage.remove(x);
          Assertions.assertNotNull(message);
        }
        Assertions.assertTrue(storage.isEmpty());
      }
    } finally {
      if (storage != null) {
        storage.delete();
      }
    }
  }

  @Test
  void basicExpiryTest() throws IOException, InterruptedException {
    Storage<MappedData> storage = null;
    try {
      storage = createStore(testName, false);
      ThreadStateContext context = new ThreadStateContext();
      context.add("domain", "ResourceAccessKey");
      ThreadLocalContext.set(context);
      // Remove any before we start

      for (int x = 0; x < 1000; x++) {
        MappedData message = createMessageBuilder(x);
        message.setExpiry(1000);
        storage.add(message);
      }
      Assertions.assertEquals(1000, storage.size());
      Thread.sleep(6000);
      Assertions.assertTrue(storage.isEmpty());
    } finally {
      if (storage != null) {
        storage.delete();
      }
    }
  }


  @Test
  void basicOpenCloseOpen() throws IOException {
    Storage<MappedData> storage = null;
    try {
      storage = createStore(testName, false);
      ThreadStateContext context = new ThreadStateContext();
      context.add("domain", "ResourceAccessKey");
      ThreadLocalContext.set(context);
      // Remove any before we start

      for (int x = 0; x < 1000; x++) {
        MappedData message = createMessageBuilder(x);
        validateMessage(message, x);
        storage.add(message);
      }
      Assertions.assertEquals(1000, storage.size());

      storage.close();
      storage = createStore(testName, false);
      Assertions.assertEquals(999, storage.getLastKey());
      for (int x = 0; x < 1000; x++) {
        MappedData message = storage.get(x);
        validateMessage(message, x);
        storage.remove(x);
        Assertions.assertNotNull(message);
      }
      Assertions.assertTrue(storage.isEmpty());
    } finally {
      if (storage != null) {
        storage.delete();
      }
    }
  }

}


