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

import io.mapsmessaging.storage.AsyncStorage;
import io.mapsmessaging.storage.Statistics;
import io.mapsmessaging.storage.StorageBuilder;
import io.mapsmessaging.utilities.threads.tasks.ThreadLocalContext;
import io.mapsmessaging.utilities.threads.tasks.ThreadStateContext;
import lombok.SneakyThrows;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class FilePerformanceTest extends BaseTest {

  private static final long EVENTS_TO_PUBLISH = 25000;
  private static final long BUFFER_SIZE = 1024 * 1024;

  public AsyncStorage<MappedData> createAsyncStore(String testName, boolean sync) throws IOException {
    File file = new File("test_file" + File.separator);
    if (!file.exists()) {
      Files.createDirectory(file.toPath());
    }
    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("Sync", "" + sync);
    properties.put("MaxPartitionSize", "" + (4L * 1024L * 1024L * 1024L)); // set to 4GB data limit
    StorageBuilder<MappedData> storageBuilder = new StorageBuilder<>();
    storageBuilder.setStorageType("Partition")
        .setFactory(getFactory())
        .setName("test_file" + File.separator + testName)
        .setProperties(properties);
    return new AsyncStorage<>(storageBuilder.build());
  }

  void threadedBasicUseCaseTest() throws Exception {

    AsyncStorage<MappedData> storage = null;
    try {
      storage = createAsyncStore("threadedBasicUseCaseTest", false);
      ThreadStateContext context = new ThreadStateContext();
      context.add("domain", "ResourceAccessKey");
      ThreadLocalContext.set(context);
      AtomicLong currentKey = new AtomicLong(0);
      AtomicLong lastRemovedKey = new AtomicLong(0);

      Thread stats = new Thread(new StatisticsMonitor(storage));
      stats.setDaemon(true);
      stats.start();

      Thread t1 = new Thread(new Writer(storage, currentKey, EVENTS_TO_PUBLISH));
      long time = System.currentTimeMillis();
      t1.start();
      t1.join();
      long writeTime = (System.currentTimeMillis() - time);

      Thread[] readerThreads = new Thread[10];
      for (int x = 0; x < readerThreads.length; x++) {
        readerThreads[x] = new Thread(new Reader(storage, lastRemovedKey, EVENTS_TO_PUBLISH));
      }

      time = System.currentTimeMillis();
      for (Thread readerThread : readerThreads) {
        readerThread.start();
      }

      for (Thread readerThread : readerThreads) {
        readerThread.join();
      }
      long readTime = (System.currentTimeMillis() - time);

      time = System.currentTimeMillis();
      storage.close();
      long closeTime = (System.currentTimeMillis() - time);

      time = System.currentTimeMillis();
      storage = createAsyncStore("threadedBasicUseCaseTest", false);
      long reOpenTime = (System.currentTimeMillis() - time);

      stats = new Thread(new StatisticsMonitor(storage));
      stats.setDaemon(true);
      stats.start();

      lastRemovedKey.set(0);
      for (int x = 0; x < readerThreads.length; x++) {
        readerThreads[x] = new Thread(new Reader(storage, lastRemovedKey, EVENTS_TO_PUBLISH));
      }

      time = System.currentTimeMillis();
      for (Thread readerThread : readerThreads) {
        readerThread.start();
      }

      for (Thread readerThread : readerThreads) {
        readerThread.join();
      }
      long reReadTime = (System.currentTimeMillis() - time);

      lastRemovedKey.set(0);
      Thread[] trimmerThreads = new Thread[10];
      for (int x = 0; x < trimmerThreads.length; x++) {
        trimmerThreads[x] = new Thread(new Trimmer(storage, lastRemovedKey, EVENTS_TO_PUBLISH));
      }

      time = System.currentTimeMillis();
      for (Thread trimmerThread : trimmerThreads) {
        trimmerThread.start();
      }

      for (Thread trimmerThread : trimmerThreads) {
        trimmerThread.join();
      }
      long removeTime = (System.currentTimeMillis() - time);

      long totalSize = EVENTS_TO_PUBLISH * BUFFER_SIZE;
      String size = convertToUnits(totalSize);
      System.err.println("Time to write " + size + writeTime + " ms");
      System.err.println("Time to read " + size + readTime + " ms");
      System.err.println("Time to close " + size + closeTime + " ms");
      System.err.println("Time to open " + size + reOpenTime + " ms");
      System.err.println("Time to reread " + size + reReadTime + " ms");
      System.err.println("Time to remove " + size + removeTime + " ms");


    } finally {
      if (storage != null) {
        Thread.sleep(1000); // Allow one more stats run
        storage.delete().get();
        Thread.sleep(2000);
      }
    }
  }

  private String convertToUnits(long value) {
    if (value > (1024L * 1024L * 1024L * 1024L)) {
      float t = value / (1024F * 1024F * 1024F * 1024F);
      return "" + t + " TB\t";
    } else if (value > (1024L * 1024L * 1024L)) {
      float t = value / (1024F * 1024F * 1024F);
      return "" + t + " GB\t";
    } else if (value > (1024L * 1024L)) {
      float t = value / (1024F * 1024F);
      return "" + t + " MB\t";
    } else if (value > (1024L)) {
      float t = value / (1024F);
      return "" + t + " KB\t";
    }
    return "" + value + " Bytes\t";
  }

  private class Writer implements Runnable {

    private final AsyncStorage<MappedData> storage;
    private final long eventsToPublish;
    private final AtomicLong currentKey;
    private final ByteBuffer bb;

    public Writer(AsyncStorage<MappedData> storage, AtomicLong currentKey, long eventsToPublish) {
      this.storage = storage;
      this.eventsToPublish = eventsToPublish;
      this.currentKey = currentKey;
      bb = ByteBuffer.allocate((int) BUFFER_SIZE);
      for (int x = 0; x < (BUFFER_SIZE) / 8; x++) {
        bb.putLong(x);
      }
      bb.flip();

    }

    @SneakyThrows
    @Override
    public void run() {
      for (int x = 0; x < eventsToPublish; x++) {
        MappedData message = createMessageBuilder(x);
        message.setData(bb);
        storage.add(message, null).get();
        currentKey.incrementAndGet();
        bb.flip();
      }
    }
  }

  private class Reader implements Runnable {

    private final AsyncStorage<MappedData> storage;
    private final long eventsToPublish;
    private final AtomicLong sharedKey;

    public Reader(AsyncStorage<MappedData> storage, AtomicLong sharedKey, long eventsToPublish) {
      this.storage = storage;
      this.eventsToPublish = eventsToPublish;
      this.sharedKey = sharedKey;
    }

    @SneakyThrows
    @Override
    public void run() {
      while (sharedKey.get() < eventsToPublish) {
        long key = sharedKey.getAndIncrement();
        MappedData data = storage.get(key).get();
        validateMessage(data, key);
      }
    }
  }

  private static final class Trimmer implements Runnable {

    private final AsyncStorage<MappedData> storage;
    private final long eventsToPublish;
    private final AtomicLong sharedKey;

    public Trimmer(AsyncStorage<MappedData> storage, AtomicLong sharedKey, long eventsToPublish) {
      this.storage = storage;
      this.eventsToPublish = eventsToPublish;
      this.sharedKey = sharedKey;
    }

    @SneakyThrows
    @Override
    public void run() {
      while (sharedKey.get() < eventsToPublish) {
        long key = sharedKey.getAndIncrement();
        storage.remove(key).get();
      }
    }
  }

  private static final class StatisticsMonitor implements Runnable {

    private final AsyncStorage<MappedData> storage;

    public StatisticsMonitor(AsyncStorage<MappedData> storage) {
      this.storage = storage;
    }

    @SneakyThrows
    @Override
    public void run() {
      while (true) {
        Thread.sleep(1000);
        Statistics statistics = storage.getStatistics().get();
        System.err.println(statistics);
      }
    }
  }

}
