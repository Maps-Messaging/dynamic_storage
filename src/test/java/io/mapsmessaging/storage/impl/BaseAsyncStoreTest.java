package io.mapsmessaging.storage.impl;

import io.mapsmessaging.storage.AsyncStorage;
import io.mapsmessaging.storage.Statistics;
import io.mapsmessaging.storage.StorageStatistics;
import io.mapsmessaging.storage.impl.cache.CacheStatistics;
import io.mapsmessaging.storage.impl.tier.memory.MemoryTierStatistics;
import io.mapsmessaging.storage.tasks.Completion;
import io.mapsmessaging.utilities.threads.tasks.ThreadLocalContext;
import io.mapsmessaging.utilities.threads.tasks.ThreadStateContext;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public abstract class BaseAsyncStoreTest extends BaseTest {

  public abstract AsyncStorage<MappedData> createAsyncStore(String testName, boolean sync) throws IOException;

  @Test
  void runSimpleAsyncStore() throws IOException, ExecutionException, InterruptedException {
    AsyncStorage<MappedData> async = createAsyncStore(testName, true);
    try {
      ThreadStateContext context = new ThreadStateContext();
      context.add("domain", "ResourceAccessKey");
      ThreadLocalContext.set(context);
      // Remove any before we start

      for (int x = 0; x < 10; x++) {
        MappedData message = createMessageBuilder(x);
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
  void simpleAutoPauseTest() throws IOException, ExecutionException, InterruptedException {
    AsyncStorage<MappedData> async = createAsyncStore(testName, false);
    async.enableAutoPause(1000); // Enable auto pause after 2 seconds

    try {
      ThreadStateContext context = new ThreadStateContext();
      context.add("domain", "ResourceAccessKey");
      ThreadLocalContext.set(context);
      // Remove any before we start

      for (int x = 0; x < 10; x++) {
        MappedData message = createMessageBuilder(x);
        Assertions.assertNotNull(async.add(message, null).get());
        Thread.sleep(2000); // Exceed the pause time
      }
      Assertions.assertEquals(10, async.size().get());

      for (int x = 0; x < 10; x++) {
        MappedData message = async.get(x, null).get();
        validateMessage(message, x);
        Thread.sleep(2000); // Exceed the pause time
        async.remove(x, null);
        Assertions.assertNotNull(message);
        Thread.sleep(2000); // Exceed the pause time
      }
      Assertions.assertTrue(async.isEmpty().get());
    } finally {
      async.delete(null).get();
    }
  }


  @Test
  void basicAsyncIndexTests() throws IOException, ExecutionException, InterruptedException {
    AsyncStorage<MappedData> async=null;
    try {
      async = createAsyncStore(testName, false);
      async.enableAutoPause(1000); // Enable auto pause after 2 seconds
      ThreadStateContext context = new ThreadStateContext();
      context.add("domain", "ResourceAccessKey");
      ThreadLocalContext.set(context);
      // Remove any before we start

      for (int x = 0; x < 1000; x++) {
        MappedData message = createMessageBuilder(x);
        async.add(message).get();
        validateMessage(message, x);
      }
      Assertions.assertEquals(1000, async.size().get());

      for(int x=0;x<1000;x++){
        Assertions.assertTrue(async.contains(x).get());
      }
      long index=0;
      for(Long key:async.getKeys().get()){
        Assertions.assertEquals(index, key);
        index++;
      }
      async.keepOnly(new ArrayList<>()).get();

      Assertions.assertTrue(async.isEmpty().get());
    } finally {
      if (async != null) {
        async.delete().get();
      }
    }
  }

  @Test
  void runSimpleAsyncCompletionStore() throws IOException, ExecutionException, InterruptedException {
    AsyncStorage<MappedData> async = createAsyncStore(testName, true);
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
        while (!completed.get()) {
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
        while (!completed.get()) {
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
        while (!completed.get()) {
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
      while (!completed.get()) {
        Thread.sleep(1);
      }
    }
  }

  @Test
  void basicKeepOnlyTest() throws IOException, ExecutionException, InterruptedException {
    AsyncStorage<MappedData> async = createAsyncStore(testName, false);
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
      for (long x = 0; x < 1000; x = x + 2) {
        keepList.add(x);
      }
      List<Long> result = async.keepOnly(keepList, null).get();
      Assertions.assertEquals(500, async.size().get());

      for (int x = 0; x < 1000; x++) {
        MappedData message = async.get(x, null).get();
        if (x % 2 == 0) {
          validateMessage(message, x);
          async.remove(x, null).get();
          Assertions.assertNotNull(message);
        } else {
          Assertions.assertNull(message);
        }
      }
      Assertions.assertTrue(async.isEmpty().get());
    } finally {
      async.delete(null).get();
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
      if (statistics instanceof CacheStatistics) {
        statistics = ((CacheStatistics) statistics).getStorageStatistics();
      }
      if(statistics instanceof MemoryTierStatistics){
        statistics = ((MemoryTierStatistics)statistics).getMemoryStatistics();
      }
      Assertions.assertEquals(1000, ((StorageStatistics) statistics).getWrites());
      Assertions.assertEquals(0, ((StorageStatistics) statistics).getReads());
      Assertions.assertEquals(0, ((StorageStatistics) statistics).getDeletes());

      for (int x = 0; x < 1000; x++) {
        storage.get(x).get();
      }
      statistics = storage.getStatistics().get();
      if (statistics instanceof CacheStatistics) {
        long cacheHit = ((CacheStatistics) statistics).getHit();
        long cacheMiss = ((CacheStatistics) statistics).getMiss();
        statistics = ((CacheStatistics) statistics).getStorageStatistics();
        if(statistics instanceof MemoryTierStatistics){
          statistics = ((MemoryTierStatistics)statistics).getMemoryStatistics();
        }
        Assertions.assertEquals(cacheMiss, ((StorageStatistics) statistics).getReads());
        Assertions.assertEquals(1000, (cacheHit + cacheMiss));
      } else {
        if(statistics instanceof MemoryTierStatistics){
          statistics = ((MemoryTierStatistics)statistics).getMemoryStatistics();
        }
        Assertions.assertEquals(1000, ((StorageStatistics) statistics).getReads());
      }
      Assertions.assertEquals(0, ((StorageStatistics) statistics).getWrites());
      Assertions.assertEquals(0, ((StorageStatistics) statistics).getDeletes());
      Assertions.assertNotNull(statistics.toString());

      for (int x = 0; x < 1000; x++) {
        storage.remove(x).get();
      }
      statistics = storage.getStatistics().get();
      if (statistics instanceof CacheStatistics) {
        statistics = ((CacheStatistics) statistics).getStorageStatistics();
      }
      if(statistics instanceof MemoryTierStatistics){
        statistics = ((MemoryTierStatistics)statistics).getMemoryStatistics();
      }
      Assertions.assertEquals(0, ((StorageStatistics) statistics).getWrites());
      Assertions.assertEquals(0, ((StorageStatistics) statistics).getReads());
      Assertions.assertEquals(1000, ((StorageStatistics) statistics).getDeletes());

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
