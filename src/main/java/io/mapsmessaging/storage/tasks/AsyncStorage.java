package io.mapsmessaging.storage.tasks;

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.utilities.threads.tasks.SingleConcurrentTaskScheduler;
import io.mapsmessaging.utilities.threads.tasks.TaskScheduler;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.concurrent.*;

public class AsyncStorage<T extends Storable> {

  private final Storage<T> storage;
  private final TaskScheduler taskScheduler;

  public AsyncStorage(@NotNull Storage<T> storage){
    this.storage = storage;
    taskScheduler = new SingleConcurrentTaskScheduler(storage.getName());
  }

  public final Future<Boolean> delete(Completion<Boolean> completion){
    return taskScheduler.submit(new DeleteTask<T>(storage, completion));
  }

  public Future<T> add(@NotNull T toStore, Completion<T> completion) {
    return taskScheduler.submit(new AddTask<>(storage, toStore, completion));
  }

  public Future<Boolean> remove(long key, Completion<Boolean> completion) {
    return taskScheduler.submit(new RemoveTask<>(storage, key, completion));
  }

  public Future<T> get(long key, Completion<T> completion){
    return taskScheduler.submit(new GetTask<>(storage, key, completion));
  }

  public Future<Long> size() {
    return taskScheduler.submit(new SizeTask<>(storage));
  }

  public Future<Boolean> isEmpty(){
    return taskScheduler.submit(new IsEmptyTask<>(storage));
  }

  // Returns a list of events NOT found but was in the to keep list
  public Future<List<Long>> keepOnly(@NotNull List<Long> listToKeep, Completion<List<Long>> completion) {
    return taskScheduler.submit(new KeepOnlyTask<>(storage, listToKeep, completion));
  }
}
