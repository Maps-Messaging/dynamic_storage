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

package io.mapsmessaging.storage.impl.debug;

import io.mapsmessaging.logging.Logger;
import io.mapsmessaging.logging.LoggerFactory;
import io.mapsmessaging.storage.*;
import io.mapsmessaging.storage.impl.file.TaskQueue;
import io.mapsmessaging.utilities.threads.tasks.TaskScheduler;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static io.mapsmessaging.storage.logging.StorageLogMessages.DEBUG_LOGGING;
import static io.mapsmessaging.storage.logging.StorageLogMessages.DEBUG_THREAD_MONITOR_LOGGING;

public class DebugStorage<T extends Storable> implements Storage<T>, ExpiredMonitor, TierMigrationMonitor {

  private final Storage<T> physicalStorage;
  private final Logger logger = LoggerFactory.getLogger(DebugStorage.class);
  private AtomicReference<Context> thread = new AtomicReference<>();

  public DebugStorage(final Storage<T> physicalStorage) {
    this.physicalStorage = physicalStorage;
  }

  @Override
  public void delete() throws IOException {
    try {
      enterFunction("delete");
      logger.log(DEBUG_LOGGING, "Delete on " + physicalStorage.getName());
      physicalStorage.delete();
    } finally {
      exitFunction();
    }
  }

  @Override
  public void shutdown() throws IOException {
    try {
      enterFunction("shutdown");
      logger.log(DEBUG_LOGGING, "Shutdown on " + physicalStorage.getName());
      physicalStorage.shutdown();
    } finally {
      exitFunction();
    }
  }

  @Override
  public String getName() {
    try {
      enterFunction("getName");
      return physicalStorage.getName();
    } finally {
      exitFunction();
    }
  }

  @Override
  public long size() throws IOException {
    try {
      enterFunction("size");
      long size = physicalStorage.size();
      logger.log(DEBUG_LOGGING, "Size of " + physicalStorage.getName() + ": " + size);
      return size;
    } finally {
      exitFunction();
    }
  }

  @Override
  public long getLastKey() {
    try {
      enterFunction("getLastKey");
      long lastKey = physicalStorage.getLastKey();
      logger.log(DEBUG_LOGGING, "Last key of " + physicalStorage.getName() + ": " + lastKey);
      return lastKey;
    } finally {
      exitFunction();
    }
  }

  @Override
  public long getLastAccess() {
    try {
      enterFunction("getLastAccess");
      long lastAccess = physicalStorage.getLastAccess();
      logger.log(DEBUG_LOGGING, "Last access of " + physicalStorage.getName() + ": " + lastAccess);
      return lastAccess;
    } finally {
      exitFunction();
    }
  }

  @Override
  public void updateLastAccess() {
    try {
      enterFunction("updateLastAccess");
      logger.log(DEBUG_LOGGING, "Update last access on " + physicalStorage.getName());
      physicalStorage.updateLastAccess();
    } finally {
      exitFunction();
    }
  }

  @Override
  public boolean isEmpty() {
    try {
      enterFunction("isEmpty");
      boolean isEmpty = physicalStorage.isEmpty();
      logger.log(DEBUG_LOGGING, physicalStorage.getName() + " is empty: " + isEmpty);
      return isEmpty;
    } finally {
      exitFunction();
    }
  }

  @Override
  public TaskQueue getTaskScheduler() {
    try {
      enterFunction("getTaskScheduler");
      return physicalStorage.getTaskScheduler();
    } finally {
      exitFunction();
    }
  }

  @Override
  public boolean supportPause() {
    try {
      enterFunction("supportPause");
      return physicalStorage.supportPause();
    } finally {
      exitFunction();
    }
  }

  @Override
  public void pause() throws IOException {
    try {
      enterFunction("pause");
      logger.log(DEBUG_LOGGING, "Pause on " + physicalStorage.getName());
      physicalStorage.pause();
    } finally {
      exitFunction();
    }
  }

  @Override
  public void resume() throws IOException {
    try {
      enterFunction("resume");
      logger.log(DEBUG_LOGGING, "Resume on " + physicalStorage.getName());
      physicalStorage.resume();
    } finally {
      exitFunction();
    }
  }

  @Override
  public boolean isCacheable() {
    try {
      enterFunction("isCacheable");
      return physicalStorage.isCacheable();
    } finally {
      exitFunction();
    }
  }

  @Override
  public @NotNull Collection<Long> keepOnly(@NotNull Collection<Long> listToKeep) throws IOException {
    try {
      enterFunction("keepOnly");
      logger.log(DEBUG_LOGGING, "Keep only on " + physicalStorage.getName() + ": " + listToKeep);
      return physicalStorage.keepOnly(listToKeep);
    } finally {
      exitFunction();
    }
  }

  @Override
  public int removeAll(@NotNull Collection<Long> listToRemove) throws IOException {
    try {
      enterFunction("removeAll");
      logger.log(DEBUG_LOGGING, "Remove all on " + physicalStorage.getName() + ": " + listToRemove);
      return physicalStorage.removeAll(listToRemove);
    } finally {
      exitFunction();
    }
  }

  @Override
  public @NotNull Statistics getStatistics() {
    try {
      enterFunction("getStatistics");
      return physicalStorage.getStatistics();
    } finally {
      exitFunction();
    }
  }

  @Override
  public void add(@NotNull T object) throws IOException {
    try {
      enterFunction("add");
      logger.log(DEBUG_LOGGING, "Add object to " + physicalStorage.getName() + ": key:" + object.getKey());
      physicalStorage.add(object);
    } finally {
      exitFunction();
    }
  }

  @Override
  public boolean remove(long key) throws IOException {
    try {
      enterFunction("remove");
      logger.log(DEBUG_LOGGING, "Remove key from " + physicalStorage.getName() + ": " + key);
      return physicalStorage.remove(key);
    } finally {
      exitFunction();
    }
  }

  @Override
  public @Nullable T get(long key) throws IOException {
    try {
      enterFunction("get");
      logger.log(DEBUG_LOGGING, "Get key from " + physicalStorage.getName() + ": " + key);
      return physicalStorage.get(key);
    } finally {
      exitFunction();
    }
  }

  @Override
  public @NotNull List<Long> getKeys() {
    try {
      enterFunction("getKeys");
      List<Long> keys = physicalStorage.getKeys();
      logger.log(DEBUG_LOGGING, "Get keys from " + physicalStorage.getName() + ": " + keys);
      return keys;
    } finally {
      exitFunction();
    }
  }

  @Override
  public boolean contains(long key) {
    try {
      enterFunction("contains");
      boolean contains = physicalStorage.contains(key);
      logger.log(DEBUG_LOGGING, "Contains key in " + physicalStorage.getName() + ": " + key + " - " + contains);
      return contains;
    } finally {
      exitFunction();
    }
  }

  @Override
  public boolean executeTasks() throws Exception {
    try {
      enterFunction("executeTasks");
      logger.log(DEBUG_LOGGING, "Execute tasks on " + physicalStorage.getName());
      return physicalStorage.executeTasks();
    } finally {
      exitFunction();
    }
  }

  @Override
  public void setExecutor(TaskScheduler executor) {
    try {
      enterFunction("setExecutor");
      logger.log(DEBUG_LOGGING, "Set executor on " + physicalStorage.getName() + ": " + executor);
      physicalStorage.setExecutor(executor);
    } finally {
      exitFunction();
    }
  }

  @Override
  public void close() throws IOException {
    try {
      enterFunction("close");
      logger.log(DEBUG_LOGGING, "Close on " + physicalStorage.getName());
      physicalStorage.close();
    } finally {
      exitFunction();
    }
  }

  @Override
  public void scanForExpired() throws IOException {
    try {
      enterFunction("scanForExpired");
      if (physicalStorage instanceof ExpiredMonitor) {
        logger.log(DEBUG_LOGGING, "Scan for expired on " + physicalStorage.getName());
        ((ExpiredMonitor) physicalStorage).scanForExpired();
      }
    } finally {
      exitFunction();
    }
  }

  @Override
  public void scanForArchiveMigration() throws IOException {
    try {
      enterFunction("scanForArchiveMigration");
      if (physicalStorage instanceof TierMigrationMonitor) {
        logger.log(DEBUG_LOGGING, "Scan for tier migration on " + physicalStorage.getName());
        ((TierMigrationMonitor) physicalStorage).scanForArchiveMigration();
      }
    } finally {
      exitFunction();
    }
  }

  private void enterFunction(String functionName) {
    Context existingContext = thread.get();
    if (existingContext != null) {
      logger.log(DEBUG_THREAD_MONITOR_LOGGING, "Detected multi-thread access while accessing " + functionName + " and " + existingContext.function);
      logger.log(DEBUG_THREAD_MONITOR_LOGGING, "External::" + produceStackTrace(existingContext.t));
      logger.log(DEBUG_THREAD_MONITOR_LOGGING, "Current::" + produceStackTrace(Thread.currentThread()));
    }
    thread.set(new Context(Thread.currentThread(), functionName));
  }

  private void exitFunction() {
    Context existingContext = thread.getAndSet(null);
    if (existingContext != null && existingContext.t != Thread.currentThread()) {
      logger.log(DEBUG_THREAD_MONITOR_LOGGING, "Thread has changed during function call");
    }
  }

  private String produceStackTrace(Thread t) {
    StringBuilder sb = new StringBuilder();
    if (t != null) {
      sb.append(t).append("\n");
      StackTraceElement[] stackTraceElements = t.getStackTrace();
      for (StackTraceElement stackTraceElement : stackTraceElements) {
        sb.append(stackTraceElement.toString()).append("\n");
      }
    }
    return sb.toString();
  }

  @Data
  @AllArgsConstructor
  private static class Context{
    Thread t;
    String function;
  }
}
