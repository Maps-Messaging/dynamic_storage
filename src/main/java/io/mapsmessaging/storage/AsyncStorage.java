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

package io.mapsmessaging.storage;

import io.mapsmessaging.logging.Logger;
import io.mapsmessaging.logging.LoggerFactory;
import io.mapsmessaging.storage.impl.tier.memory.MemoryTierStorage;
import io.mapsmessaging.storage.logging.StorageLogMessages;
import io.mapsmessaging.storage.tasks.AddTask;
import io.mapsmessaging.storage.tasks.AutoPauseTask;
import io.mapsmessaging.storage.tasks.CloseTask;
import io.mapsmessaging.storage.tasks.Completion;
import io.mapsmessaging.storage.tasks.DeleteTask;
import io.mapsmessaging.storage.tasks.GetTask;
import io.mapsmessaging.storage.tasks.IsEmptyTask;
import io.mapsmessaging.storage.tasks.KeepOnlyTask;
import io.mapsmessaging.storage.tasks.LastKeyTask;
import io.mapsmessaging.storage.tasks.PauseTask;
import io.mapsmessaging.storage.tasks.RemoveTask;
import io.mapsmessaging.storage.tasks.RetrieveStatisticsTask;
import io.mapsmessaging.storage.tasks.SizeTask;
import io.mapsmessaging.utilities.threads.tasks.PriorityConcurrentTaskScheduler;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;

public class AsyncStorage<T extends Storable> implements Closeable {

  private final Logger logger;

  private static final int BACKGROUND_PRIORITY = 0;
  private static final int FOREGROUND_PRIORITY = 1;

  private final Storage<T> storage;
  private final PriorityConcurrentTaskScheduler scheduler;
  private final AtomicBoolean closed;

  private ScheduledFuture<?> autoPauseFuture;

  public AsyncStorage(@NotNull Storage<T> storage) {
    this.storage = storage;
    scheduler = new PriorityConcurrentTaskScheduler(storage.getName(), 2);
    closed = new AtomicBoolean(false);
    storage.setExecutor(scheduler);
    autoPauseFuture = null;
    logger = LoggerFactory.getLogger("AsyncStorage - "+storage.getName());
    logger.log(StorageLogMessages.ASYNC_STORAGE_CREATED, storage.getName());
  }

  @SneakyThrows
  @Override
  public void close() throws IOException {
    Future<Boolean> future = close(null);
    try {
      future.get();
      logger.log(StorageLogMessages.ASYNC_CLOSE_COMPLETED);
    } catch (ExecutionException e) {
      logger.log(StorageLogMessages.ASYNC_CLOSE_FAILED, e);
      throw new IOException(e);
    }
  }

  public final Future<Boolean> close(Completion<Boolean> completion) throws IOException {
    checkClose();
    if (autoPauseFuture != null) {
      autoPauseFuture.cancel(false);
    }
    logger.log(StorageLogMessages.ASYNC_CLOSE_REQUESTED);
    closed.set(true);
    storage.shutdown();
    return scheduler.submit(new CloseTask<>(storage, completion), FOREGROUND_PRIORITY);
  }

  public void enableAutoPause(long idleTime) {
    logger.log(StorageLogMessages.ASYNC_ENABLE_AUTO_PAUSE);
    if(storage instanceof MemoryTierStorage){
      long migrationTime = ((MemoryTierStorage<T>)storage).getMigrationTime();
      if(migrationTime > idleTime){ // We would pause the file store before migrating from memory to file
        idleTime = (long)(migrationTime * 1.5f);
      }
    }
    AutoPauseTask autoPauseTask = new AutoPauseTask(this, idleTime);
    autoPauseFuture = storage.getTaskScheduler().scheduleAtFixedRate(autoPauseTask, idleTime, TimeUnit.MILLISECONDS);
  }

  public final Future<Boolean> delete() throws IOException {
    return delete(null);
  }

  public final Future<Boolean> delete(Completion<Boolean> completion) throws IOException {
    checkClose();
    if (autoPauseFuture != null) {
      autoPauseFuture.cancel(false);
    }
    storage.getTaskScheduler().purge();
    logger.log(StorageLogMessages.ASYNC_DELETE_REQUESTED);
    closed.set(true);
    storage.shutdown();
    return scheduler.submit(new DeleteTask<>(storage, completion), FOREGROUND_PRIORITY);
  }

  public final Future<T> add(@NotNull T toStore) throws IOException {
    return add(toStore, null);
  }

  public Future<T> add(@NotNull T toStore, Completion<T> completion) throws IOException {
    checkClose();
    logger.log(StorageLogMessages.ASYNC_ADD_REQUESTED, toStore.getKey());
    return scheduler.submit(new AddTask<>(storage, toStore, completion), FOREGROUND_PRIORITY);
  }

  public Future<Boolean> remove(long key) throws IOException {
    return remove(key, null);
  }

  public Future<Boolean> remove(long key, Completion<Boolean> completion) throws IOException {
    checkClose();
    logger.log(StorageLogMessages.ASYNC_REMOVE_REQUESTED, key);
    return scheduler.submit(new RemoveTask<>(storage, key, completion), FOREGROUND_PRIORITY);
  }

  public Future<T> get(long key) throws IOException {
    return get(key, null);
  }

  public Future<T> get(long key, Completion<T> completion) throws IOException {
    checkClose();
    logger.log(StorageLogMessages.ASYNC_GET_REQUESTED, key);
    return scheduler.submit(new GetTask<>(storage, key, completion), FOREGROUND_PRIORITY);
  }

  public Future<Long> size() throws IOException {
    checkClose();
    logger.log(StorageLogMessages.ASYNC_SIZE_REQUESTED);
    return scheduler.submit(new SizeTask<>(storage), FOREGROUND_PRIORITY);
  }

  public Future<Long> getLastKey() throws IOException {
    checkClose();
    logger.log(StorageLogMessages.ASYNC_LAST_KEY_REQUESTED);
    return scheduler.submit(new LastKeyTask<>(storage), FOREGROUND_PRIORITY);
  }

  public Future<Boolean> isEmpty() {
    logger.log(StorageLogMessages.ASYNC_IS_EMPTY_REQUESTED);
    return scheduler.submit(new IsEmptyTask<>(storage), FOREGROUND_PRIORITY);
  }

  // Returns a list of events NOT found but was in the to keep list
  public Future<List<Long>> keepOnly(@NotNull List<Long> listToKeep) throws IOException {
    return keepOnly(listToKeep, null);
  }

  public Future<List<Long>> keepOnly(@NotNull List<Long> listToKeep, Completion<List<Long>> completion) throws IOException {
    checkClose();
    return scheduler.submit(new KeepOnlyTask<>(storage, listToKeep, completion), FOREGROUND_PRIORITY);
  }

  public Future<Statistics> getStatistics() {
    return getStatistics(null);
  }

  public Future<Statistics> getStatistics(Completion<Statistics> completion) {
    logger.log(StorageLogMessages.ASYNC_STATISTICS_REQUESTED);
    return scheduler.submit(new RetrieveStatisticsTask<>(storage, completion), FOREGROUND_PRIORITY);
  }

  public Future<Void> pause() throws IOException {
    checkClose();
    logger.log(StorageLogMessages.ASYNC_PAUSE_REQUESTED);
    return scheduler.submit(new PauseTask<>(storage), BACKGROUND_PRIORITY);
  }

  public long getLastAccess() {
    return storage.getLastAccess();
  }

  protected void checkClose() throws IOException {
    if (closed.get()) {
      logger.log(StorageLogMessages.ASYNC_REQUEST_ON_CLOSED_STORE);
      throw new IOException("Store has been scheduled to close");
    }
  }

}
