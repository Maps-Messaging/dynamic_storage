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

package io.mapsmessaging.storage.tasks;

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.utilities.threads.tasks.SingleConcurrentTaskScheduler;
import io.mapsmessaging.utilities.threads.tasks.TaskScheduler;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;

public class AsyncStorage<T extends Storable> implements Closeable {

  private final Storage<T> storage;
  private final TaskScheduler taskScheduler;
  private final AtomicBoolean closed;

  public AsyncStorage(@NotNull Storage<T> storage) {
    this.storage = storage;
    taskScheduler = new SingleConcurrentTaskScheduler(storage.getName());
    closed = new AtomicBoolean(false);
  }

  @SneakyThrows
  @Override
  public void close() throws IOException {
    Future<Boolean> future = close(null);
    try {
      future.get();
    } catch (ExecutionException e) {
      throw new IOException(e);
    }
  }

  public final Future<Boolean> close(Completion<Boolean> completion) throws IOException {
    checkClose();
    closed.set(true);
    return taskScheduler.submit(new CloseTask<T>(storage, completion));
  }

  public final Future<Boolean> delete(Completion<Boolean> completion) throws IOException {
    checkClose();
    closed.set(true);
    return taskScheduler.submit(new DeleteTask<T>(storage, completion));
  }

  public Future<T> add(@NotNull T toStore, Completion<T> completion) throws IOException {
    checkClose();
    return taskScheduler.submit(new AddTask<>(storage, toStore, completion));
  }

  public Future<Boolean> remove(long key, Completion<Boolean> completion) throws IOException {
    checkClose();
    return taskScheduler.submit(new RemoveTask<>(storage, key, completion));
  }

  public Future<T> get(long key, Completion<T> completion) throws IOException {
    checkClose();
    return taskScheduler.submit(new GetTask<>(storage, key, completion));
  }

  public Future<Long> size() throws IOException {
    checkClose();
    return taskScheduler.submit(new SizeTask<>(storage));
  }

  public Future<Boolean> isEmpty() throws IOException {
    checkClose();
    return taskScheduler.submit(new IsEmptyTask<>(storage));
  }

  // Returns a list of events NOT found but was in the to keep list
  public Future<List<Long>> keepOnly(@NotNull List<Long> listToKeep, Completion<List<Long>> completion) throws IOException {
    checkClose();
    return taskScheduler.submit(new KeepOnlyTask<>(storage, listToKeep, completion));
  }

  protected void checkClose() throws IOException {
    if (closed.get()) {
      throw new IOException("Store has been scheduled to close");
    }
  }
}
