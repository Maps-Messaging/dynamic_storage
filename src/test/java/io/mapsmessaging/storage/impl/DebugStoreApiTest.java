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

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import io.mapsmessaging.storage.*;
import io.mapsmessaging.storage.impl.debug.DebugStorage;
import io.mapsmessaging.storage.impl.file.TaskQueue;
import io.mapsmessaging.utilities.threads.tasks.PriorityConcurrentTaskScheduler;
import io.mapsmessaging.utilities.threads.tasks.TaskScheduler;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

class DebugStoreApiTest {

  private TestLogAppender testLogAppender;

  @BeforeEach
  public void setup() {
    Logger logger = (Logger) LoggerFactory.getLogger(DebugStorage.class);
    testLogAppender = new TestLogAppender();
    testLogAppender.start();
    logger.addAppender(testLogAppender);
  }

  @Test
  void basicFunctionality() throws Exception {
    DebugStorage<BaseTest.MappedData> storage = new DebugStorage<>(new DebugTestStorage<>());
    storage.add(new BaseTest.MappedData());
    Assertions.assertFalse(testLogAppender.getLogEvents().isEmpty());
    testLogAppender.clear();
    storage.size();
    Assertions.assertFalse(testLogAppender.getLogEvents().isEmpty());
    testLogAppender.clear();
    storage.shutdown();
    Assertions.assertFalse(testLogAppender.getLogEvents().isEmpty());
    testLogAppender.clear();
    storage.close();
    Assertions.assertFalse(testLogAppender.getLogEvents().isEmpty());
    testLogAppender.clear();
    storage.scanForArchiveMigration();
    Assertions.assertFalse(testLogAppender.getLogEvents().isEmpty());
    testLogAppender.clear();


    storage.delete();
    Assertions.assertFalse(testLogAppender.getLogEvents().isEmpty());
    testLogAppender.clear();
    storage.contains(1);
    Assertions.assertFalse(testLogAppender.getLogEvents().isEmpty());
    testLogAppender.clear();
    storage.get(1);
    Assertions.assertFalse(testLogAppender.getLogEvents().isEmpty());
    testLogAppender.clear();
    storage.remove(1);
    Assertions.assertFalse(testLogAppender.getLogEvents().isEmpty());
    testLogAppender.clear();
    List<Long> list = new ArrayList<>();
    list.add(1L);
    storage.keepOnly(list);
    Assertions.assertFalse(testLogAppender.getLogEvents().isEmpty());
    testLogAppender.clear();
    storage.getLastKey();
    Assertions.assertFalse(testLogAppender.getLogEvents().isEmpty());
    testLogAppender.clear();
    storage.getLastAccess();
    Assertions.assertFalse(testLogAppender.getLogEvents().isEmpty());
    testLogAppender.clear();
    storage.updateLastAccess();
    Assertions.assertFalse(testLogAppender.getLogEvents().isEmpty());
    testLogAppender.clear();
    storage.getKeys();
    Assertions.assertFalse(testLogAppender.getLogEvents().isEmpty());
    testLogAppender.clear();
    storage.executeTasks();
    Assertions.assertFalse(testLogAppender.getLogEvents().isEmpty());
    testLogAppender.clear();

    storage.scanForExpired();
    Assertions.assertFalse(testLogAppender.getLogEvents().isEmpty());
    testLogAppender.clear();

    storage.pause();
    Assertions.assertFalse(testLogAppender.getLogEvents().isEmpty());
    testLogAppender.clear();
    storage.resume();
    Assertions.assertFalse(testLogAppender.getLogEvents().isEmpty());
    testLogAppender.clear();

    storage.removeAll(list);
    Assertions.assertFalse(testLogAppender.getLogEvents().isEmpty());
    testLogAppender.clear();

    storage.isEmpty();
    Assertions.assertFalse(testLogAppender.getLogEvents().isEmpty());
    testLogAppender.clear();

    TaskScheduler taskScheduler = new PriorityConcurrentTaskScheduler("test",16);
    storage.setExecutor( taskScheduler);
    Assertions.assertFalse(testLogAppender.getLogEvents().isEmpty());
    testLogAppender.clear();

    Assertions.assertNull(storage.getTaskScheduler());

    Assertions.assertFalse(storage.supportPause());

    Assertions.assertFalse(storage.isCacheable());
    Assertions.assertEquals("Debug Test Storage", storage.getName());
    Assertions.assertNotNull(storage.getStatistics());

  }

  @AfterEach
  void detachAppender() {
    ((Logger) LoggerFactory.getLogger(DebugStorage.class)).detachAppender(testLogAppender);
    testLogAppender.stop();
  }

  @Test
  void testThreadAccessLogging() throws Exception {
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    AtomicReference<Throwable> workerFailure = new AtomicReference<>();
    DebugTestStorage<BaseTest.MappedData> base = new DebugTestStorage<>() {
      @Override
      public void close() throws IOException {
        entered.countDown();
        try {
          if (!release.await(5, TimeUnit.SECONDS)) throw new IOException("Worker release timed out");
        } catch (InterruptedException exception) {
          Thread.currentThread().interrupt();
          throw new IOException(exception);
        }
      }
    };
    DebugStorage<BaseTest.MappedData> storage = new DebugStorage<>(base);
    Thread worker = new Thread(() -> {
      try {
        storage.close();
      } catch (Throwable exception) {
        workerFailure.set(exception);
      }
    });
    worker.start();
    try {
      Assertions.assertTrue(entered.await(5, TimeUnit.SECONDS));
      testLogAppender.clear();
      storage.size();
      Assertions.assertTrue(testLogAppender.getLogEvents().stream()
          .anyMatch(event -> event.getFormattedMessage().contains("Detected multi-thread access")));
      Assertions.assertTrue(testLogAppender.getLogEvents().stream()
          .anyMatch(event -> event.getFormattedMessage().contains("External::")));
    } finally {
      release.countDown();
      worker.join(5000);
    }
    Assertions.assertFalse(worker.isAlive());
    Assertions.assertNull(workerFailure.get());
  }


  public class TestLogAppender extends AppenderBase<ILoggingEvent> {
    private final List<ILoggingEvent> logEvents = new CopyOnWriteArrayList<>();

    @Override
    protected void append(ILoggingEvent eventObject) {
      logEvents.add(eventObject);
    }

    public void clear(){
      logEvents.clear();
    }

    public List<ILoggingEvent> getLogEvents() {
      return logEvents;
    }
  }

  static class DebugTestStorage <T extends Storable> implements Storage<T>, ExpiredMonitor, TierMigrationMonitor {

    @Override
    public void scanForExpired() throws IOException {
      // these are no op functions,
    }

    @Override
    public void delete() throws IOException {
      // No operation required by the test storage.

    }

    @Override
    public String getName() {
      return "Debug Test Storage";
    }

    @Override
    public long size() throws IOException {
      return 0;
    }

    @Override
    public long getLastKey() {
      return 0;
    }

    @Override
    public long getLastAccess() {
      return 0;
    }

    @Override
    public boolean isEmpty() {
      return false;
    }

    @Override
    public boolean supportPause(){
      return false;
    }

    @Override
    public TaskQueue getTaskScheduler() {
      return null;
    }

    @Override
    public @NotNull Collection<Long> keepOnly(@NotNull Collection<Long> listToKeep) throws IOException {
      return List.of();
    }

    @Override
    public @NotNull int removeAll(@NotNull Collection<Long> listToRemove) throws IOException {
      return 0;
    }

    @Override
    public boolean isCacheable() {
      return false;
    }
    @Override
    public @NotNull Statistics getStatistics() {
      return new Statistics() {
        @Override
        public int hashCode() {
          return super.hashCode();
        }
      };
    }

    @Override
    public void add(@NotNull T object) throws IOException {

    }

    @Override
    public boolean remove(long key) throws IOException {
      return false;
    }

    @Override
    public @Nullable T get(long key) throws IOException {
      return null;
    }

    @Override
    public @NotNull List<Long> getKeys() {
      return List.of();
    }

    @Override
    public boolean contains(long key) {
      return false;
    }

    @Override
    public void setExecutor(TaskScheduler executor) {

    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void scanForArchiveMigration() throws IOException {

    }
  }
}
