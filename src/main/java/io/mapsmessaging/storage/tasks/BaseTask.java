package io.mapsmessaging.storage.tasks;

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import java.util.concurrent.Callable;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;

public abstract class BaseTask<T extends Storable, V> implements Callable<V> {

  protected final @Getter
  Storage<T> storage;
  protected final @Getter
  Completion<V> completion;

  protected BaseTask(@NotNull Storage<T> storage, Completion<V> completion) {
    this.storage = storage;
    this.completion = completion;
  }

  protected abstract V execute() throws Exception;

  @Override
  public V call() throws Exception {
    V result = null;
    Exception exception = null;
    try {
      result = execute();
    } catch (Exception e) {
      exception = e;
    }
    if (completion != null) {
      if (result != null) {
        completion.onCompletion(result);
      }
      if (exception != null) {
        completion.onException(exception);
        throw exception;
      }
    }
    return result;
  }

}
