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
import io.mapsmessaging.storage.StorageFactoryFactory;
import io.mapsmessaging.storage.tasks.AsyncStorage;
import io.mapsmessaging.utilities.threads.tasks.ThreadLocalContext;
import io.mapsmessaging.utilities.threads.tasks.ThreadStateContext;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SimpleAsyncTest extends BaseTest {


  @Test
  void runSimpleAsyncStore() throws IOException, ExecutionException, InterruptedException {
    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("Sync", "true");
    properties.put("basePath", "./test.db");
    Storage<MappedData> store = StorageFactoryFactory.getInstance().create("MapDB", properties, getFactory()).create("Test");
    AsyncStorage<MappedData> async = new AsyncStorage<>(store);


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
}
