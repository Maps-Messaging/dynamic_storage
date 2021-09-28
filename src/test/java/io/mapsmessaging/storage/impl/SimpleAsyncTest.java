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

import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.storage.StorageBuilder;
import io.mapsmessaging.storage.AsyncStorage;
import io.mapsmessaging.storage.tasks.Completion;
import io.mapsmessaging.utilities.threads.tasks.ThreadLocalContext;
import io.mapsmessaging.utilities.threads.tasks.ThreadStateContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SimpleAsyncTest extends BaseTest {


  @Test
  void runSimpleAsyncStore() throws IOException, ExecutionException, InterruptedException {
    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("Sync", "true");
    properties.put("basePath", "./test.db");
    StorageBuilder<MappedData> storageBuilder = new StorageBuilder<>();
    storageBuilder.setStorageType("Partition")
        .setFactory(getFactory())
        .setName("Test")
        .setProperties(properties);
    AsyncStorage<MappedData> async =storageBuilder.buildAsync();


    try {
      ThreadStateContext context = new ThreadStateContext();
      context.add("domain", "ResourceAccessKey");
      ThreadLocalContext.set(context);
      // Remove any before we start

      for (int x = 0; x < 10; x++) {
        MappedData message = createMessageBuilder(x);
        validateMessage(message, x);
        Assertions.assertNotNull(async.add(message, null).get());
      }
      Assertions.assertEquals(10, async.size().get());

      for (int x = 0; x < 10; x++) {
        MappedData message = async.get(x, null).get();
        validateMessage(message, x);
        async.remove(x, null);
        Assertions.assertNotNull(message);
      }
      Assertions.assertTrue(async.isEmpty().get());
    } finally {
      async.delete(null).get();
    }

  }


  @Test
  void runSimpleAsyncCompletionStore() throws IOException, ExecutionException, InterruptedException {
    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("Sync", "true");
    properties.put("basePath", "./test.db");
    StorageBuilder<MappedData> storageBuilder = new StorageBuilder<>();
    storageBuilder.setStorageType("Partition")
        .setFactory(getFactory())
        .setName("Test")
        .setProperties(properties);
    AsyncStorage<MappedData> async =storageBuilder.buildAsync();
    AtomicBoolean completed = new AtomicBoolean(false);


    try {
      ThreadStateContext context = new ThreadStateContext();
      context.add("domain", "ResourceAccessKey");
      ThreadLocalContext.set(context);
      // Remove any before we start

      for (int x = 0; x < 10; x++) {
        MappedData message = createMessageBuilder(x);
        validateMessage(message, x);
        completed.set(false);
        async.add(message, new Completion<>() {
          @Override
          public void onCompletion(MappedData result) {
            Assertions.assertNotNull(result);
            completed.set(true);
          }

          @Override
          public void onException(Exception exception) {
            Assertions.fail(exception);
          }
        });
        while(!completed.get()){
          Thread.sleep(1);
        }
      }

      Assertions.assertEquals(10, async.size().get());

      for (int x = 0; x < 10; x++) {
        completed.set(false);
        int finalX = x;
        async.get(x, new Completion<>() {
          @Override
          public void onCompletion(MappedData result) {
            completed.set(true);
            Assertions.assertNotNull(result);
            validateMessage(result, finalX);
          }

          @Override
          public void onException(Exception exception) {
            completed.set(true);
            Assertions.fail(exception);
          }
        });
        while(!completed.get()){
          Thread.sleep(1);
        }

        completed.set(false);
        async.remove(x, new Completion<>() {
          @Override
          public void onCompletion(Boolean result) {
            completed.set(true);
            Assertions.assertTrue(result);
          }

          @Override
          public void onException(Exception exception) {
            completed.set(true);
            Assertions.fail(exception);
          }
        });
        while(!completed.get()){
          Thread.sleep(1);
        }
      }
      Assertions.assertTrue(async.isEmpty().get());
    } finally {
      async.delete(new Completion<>() {
        @Override
        public void onCompletion(Boolean result) {
          Assertions.assertTrue(result);
          completed.set(true);
        }

        @Override
        public void onException(Exception exception) {
          Assertions.fail(exception);
        }
      });
      while(!completed.get()){
        Thread.sleep(1);
      }
    }
  }

  @Test
  void basicKeepOnlyTest() throws IOException, ExecutionException, InterruptedException {
    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("Sync", "true");
    properties.put("basePath", "./test.db");
    StorageBuilder<MappedData> storageBuilder = new StorageBuilder<>();
    storageBuilder.setStorageType("Partition")
        .setFactory(getFactory())
        .setName("Test")
        .setProperties(properties);
    AsyncStorage<MappedData> async =storageBuilder.buildAsync();
    try {
      ThreadStateContext context = new ThreadStateContext();
      context.add("domain", "ResourceAccessKey");
      ThreadLocalContext.set(context);
      // Remove any before we start

      for (int x = 0; x < 1000; x++) {
        MappedData message = createMessageBuilder(x);
        validateMessage(message, x);
        Assertions.assertNotNull(async.add(message, null).get());
      }
      Assertions.assertEquals(1000, async.size().get());

      List<Long> keepList = new ArrayList<>();
      for(long x=0;x<1000;x=x+2){
        keepList.add(x);
      }
      List<Long> result = async.keepOnly(keepList, null).get();
      Assertions.assertEquals(500, async.size().get());


      for (int x = 0; x < 1000; x++) {
        MappedData message = async.get(x, null).get();
        if(x%2 == 0){
          validateMessage(message, x);
          async.remove(x, null).get();
          Assertions.assertNotNull(message);
        }
        else{
          Assertions.assertNull(message);
        }
      }
      Assertions.assertTrue(async.isEmpty().get());
    } finally {
      async.delete(null).get();
    }
  }

}
