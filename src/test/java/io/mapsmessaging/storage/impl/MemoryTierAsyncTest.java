package io.mapsmessaging.storage.impl;

import io.mapsmessaging.storage.AsyncStorage;
import io.mapsmessaging.storage.Storage;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MemoryTierAsyncTest extends BaseAsyncStoreTest {

  @Override
  public AsyncStorage<MappedData> createAsyncStore(String testName, boolean sync) throws IOException {
    Storage<MappedData> storage = MemoryTierTest.build(testName, sync);
    return new AsyncStorage<>(storage);
  }


  @Test
  void testTierMigration() throws IOException, ExecutionException, InterruptedException {
    AsyncStorage<MappedData> store = createAsyncStore(testName, false);
    try{
      for(int x=0;x<1000;x++){
        MappedData message = createMessageBuilder(x);
        Assertions.assertNotNull(store.add(message).get());
      }
      for(int x=0;x<100;x++){
        store.remove(x).get();
      }
      // OK this should be in the memory tier...
      Thread.sleep(12000);
      // things should have migrated...
      for (int x = 100; x < 1000; x++) {
        MappedData message = store.get(x).get();
        validateMessage(message, x);
        store.remove(x);
        Assertions.assertNotNull(message);
      }
    }
    finally {
      store.delete();
    }
  }
}
