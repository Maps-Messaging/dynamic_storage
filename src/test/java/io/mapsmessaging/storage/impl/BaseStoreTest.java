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

import io.mapsmessaging.storage.AsyncStorage;
import io.mapsmessaging.storage.Statistics;
import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.storage.StorageStatistics;
import io.mapsmessaging.storage.impl.cache.CacheStatistics;
import io.mapsmessaging.utilities.threads.tasks.ThreadLocalContext;
import io.mapsmessaging.utilities.threads.tasks.ThreadStateContext;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;


public abstract class BaseStoreTest extends BaseTest {

  public abstract Storage<MappedData> createStore(String testName, boolean sync) throws IOException;

  public abstract AsyncStorage<MappedData> createAsyncStore(String testName, boolean sync) throws IOException;

  private String testName;

  @BeforeEach
  public void beforeEachTest(final TestInfo testInfo) {
    testName = testInfo.getDisplayName();
  }

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
        MappedData message = createMessageBuilder(x);
        validateMessage(message, x);
        storage.add(message);
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
  void basicPauseResume()throws IOException {
    Storage<MappedData> storage = null;
    try {
      storage = createStore(testName, false);
      if(storage.supportPause()) {
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

  @Test
  void basicUseCaseTest() throws Exception {
    AsyncStorage<MappedData> storage = null;
    try {
      storage = createAsyncStore(testName, false);
      ThreadStateContext context = new ThreadStateContext();
      context.add("domain", "ResourceAccessKey");
      ThreadLocalContext.set(context);
      // Remove any before we start

      Assertions.assertEquals(0, storage.getLastKey().get());
      int counter = -10000;
      for (int x = 0; x < 100000; x++) {
        MappedData message = createMessageBuilder(x);
        storage.add(message, null).get();
        counter++;
        if (counter >= 0) {
          storage.remove(counter).get();
        }
      }
      Assertions.assertEquals(99999, storage.getLastKey().get());
      for (int x = counter; x < 100000; x++) {
        storage.remove(x).get();
      }
      Assertions.assertEquals(0, storage.size().get());
      Assertions.assertEquals(99999, storage.getLastKey().get());
    } finally {
      if (storage != null) {
        storage.delete().get();
      }
    }
  }


  @Test
  void simpleStatsTest() throws Exception {
    AsyncStorage<MappedData> storage = null;
    try {
      storage = createAsyncStore(testName, false);
      ThreadStateContext context = new ThreadStateContext();
      context.add("domain", "ResourceAccessKey");
      ThreadLocalContext.set(context);
      // Remove any before we start

      for (int x = 0; x < 1000; x++) {
        MappedData message = createMessageBuilder(x);
        storage.add(message, null).get();
      }
      Statistics statistics = storage.getStatistics().get();
      if(statistics instanceof CacheStatistics){
        statistics = ((CacheStatistics)statistics).getStorageStatistics();
      }
      Assertions.assertEquals(1000, ((StorageStatistics)statistics).getWrites());
      Assertions.assertEquals(0, ((StorageStatistics)statistics).getReads());
      Assertions.assertEquals(0, ((StorageStatistics)statistics).getDeletes());

      for (int x = 0; x < 1000; x++) {
        storage.get(x).get();
      }
      statistics = storage.getStatistics().get();
      if(statistics instanceof CacheStatistics){
        long cacheHit = ((CacheStatistics)statistics).getHit();
        long cacheMiss = ((CacheStatistics)statistics).getMiss();
        statistics = ((CacheStatistics)statistics).getStorageStatistics();
        Assertions.assertEquals(cacheMiss, ((StorageStatistics)statistics).getReads());
        Assertions.assertEquals(1000, (cacheHit+cacheMiss) );
      }
      else{
        Assertions.assertEquals(1000, ((StorageStatistics)statistics).getReads());
      }
      Assertions.assertEquals(0, ((StorageStatistics)statistics).getWrites());
      Assertions.assertEquals(0, ((StorageStatistics)statistics).getDeletes());
      Assertions.assertNotNull(statistics.toString());

      for (int x = 0; x < 1000; x++) {
        storage.remove(x).get();
      }
      statistics = storage.getStatistics().get();
      if(statistics instanceof CacheStatistics){
        statistics = ((CacheStatistics)statistics).getStorageStatistics();
      }
      Assertions.assertEquals(0, ((StorageStatistics)statistics).getWrites());
      Assertions.assertEquals(0, ((StorageStatistics)statistics).getReads());
      Assertions.assertEquals(1000, ((StorageStatistics)statistics).getDeletes());

      Assertions.assertEquals(0, storage.size().get());
    } finally {
      if (storage != null) {
        storage.delete().get();
      }
    }
  }

  @Test
  void basicLargeDataUseCaseTest() throws Exception {
    AsyncStorage<MappedData> storage = null;
    int iterations = 5;
    try {
      storage = createAsyncStore(testName, false);
      ThreadStateContext context = new ThreadStateContext();
      context.add("domain", "ResourceAccessKey");
      ThreadLocalContext.set(context);
      // Remove any before we start
      int size = 512 * 1024; //1GB
      ByteBuffer bb = ByteBuffer.allocate(size);
      long y = 0;
      while (bb.limit() - bb.position() > 8) {
        bb.putLong(y);
        y++;
      }
      for (int x = 0; x < iterations; x++) {
        bb.flip();
        MappedData message = createMessageBuilder(x);
        message.setData(bb);
        storage.add(message).get();
      }
      Assertions.assertEquals(iterations, storage.size().get());
      for (int x = 0; x < iterations; x++) {
        MappedData mappedData = storage.get(x).get();
        validateMessage(mappedData, x);
      }
      for (int x = 0; x < iterations; x++) {
        storage.remove(x).get();
      }
    } finally {
      if (storage != null) {
        storage.delete().get();
      }
    }
  }
}


