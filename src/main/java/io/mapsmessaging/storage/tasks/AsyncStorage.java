package io.mapsmessaging.storage.tasks;

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.utilities.threads.tasks.SingleConcurrentTaskScheduler;
import io.mapsmessaging.utilities.threads.tasks.TaskScheduler;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.concurrent.*;

public class AsyncStorage<T extends Storable> implements Closeable {

  private final Storage<T> storage;
  private final TaskScheduler taskScheduler;
  private final AtomicBoolean closed;

  public AsyncStorage(@NotNull Storage<T> storage){
    this.storage = storage;
    taskScheduler = new SingleConcurrentTaskScheduler(storage.getName());
    closed = new AtomicBoolean(false);
  }

  @Override
  public void close() throws IOException {
    Future<Boolean> future = close(null);
    try {
      future.get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException(e);
    }
  }

  public final Future<Boolean> close(Completion<Boolean> completion) throws IOException {
    if(closed.get()){
      throw new IOException("Store has been scheduled to close");
    }
    closed.set(true);
    return taskScheduler.submit(new CloseTask<T>(storage, completion));
  }

  public final Future<Boolean> delete(Completion<Boolean> completion) throws IOException {
    if(closed.get()){
      throw new IOException("Store has been scheduled to close");
    }
    closed.set(true);
    return taskScheduler.submit(new DeleteTask<T>(storage, completion));
  }

  public Future<T> add(@NotNull T toStore, Completion<T> completion) throws IOException {
    if(closed.get()){
      throw new IOException("Store has been scheduled to close");
    }
    return taskScheduler.submit(new AddTask<>(storage, toStore, completion));
  }

  public Future<Boolean> remove(long key, Completion<Boolean> completion) throws IOException {
    if(closed.get()){
      throw new IOException("Store has been scheduled to close");
    }
    return taskScheduler.submit(new RemoveTask<>(storage, key, completion));
  }

  public Future<T> get(long key, Completion<T> completion) throws IOException {
    if(closed.get()){
      throw new IOException("Store has been scheduled to close");
    }
    return taskScheduler.submit(new GetTask<>(storage, key, completion));
  }

  public Future<Long> size() throws IOException {
    if(closed.get()){
      throw new IOException("Store has been scheduled to close");
    }
    return taskScheduler.submit(new SizeTask<>(storage));
  }

  public Future<Boolean> isEmpty() throws IOException {
    if(closed.get()){
      throw new IOException("Store has been scheduled to close");
    }
    return taskScheduler.submit(new IsEmptyTask<>(storage));
  }

  // Returns a list of events NOT found but was in the to keep list
  public Future<List<Long>> keepOnly(@NotNull List<Long> listToKeep, Completion<List<Long>> completion) throws IOException {
    if(closed.get()){
      throw new IOException("Store has been scheduled to close");
    }
    return taskScheduler.submit(new KeepOnlyTask<>(storage, listToKeep, completion));
  }
}
