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

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import io.mapsmessaging.storage.ExpiredMonitor;
import io.mapsmessaging.storage.Statistics;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.storage.impl.debug.DebugStorage;
import io.mapsmessaging.storage.impl.file.TaskQueue;
import io.mapsmessaging.utilities.threads.tasks.TaskScheduler;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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
  }


  public class TestLogAppender extends AppenderBase<ILoggingEvent> {
    private final List<ILoggingEvent> logEvents = new ArrayList<>();

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

  static class DebugTestStorage <T extends Storable> implements Storage<T>, ExpiredMonitor {

    @Override
    public void scanForExpired() throws IOException {
      // these are no op functions,
    }

    @Override
    public void delete() throws IOException {
      // these are no op functions,

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
  }
}
