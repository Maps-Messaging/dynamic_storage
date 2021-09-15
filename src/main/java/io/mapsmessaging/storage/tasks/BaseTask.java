package io.mapsmessaging.storage.tasks;

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.Callable;

public abstract class BaseTask<T extends Storable, V> implements Callable<V> {
  protected final @Getter Storage<T> storage;
  protected final @Getter Completion<V> completion;

  protected BaseTask(@NotNull Storage<T> storage, Completion<V> completion){
    this.storage = storage;
    this.completion = completion;
  }

  protected abstract V execute() throws Exception;

  @Override
  public V call() throws Exception {
    V result = execute();
    if(completion != null){
      completion.complete(result);
    }
    return result;
  }

}
