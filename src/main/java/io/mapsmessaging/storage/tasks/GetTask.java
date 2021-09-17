package io.mapsmessaging.storage.tasks;

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import org.jetbrains.annotations.NotNull;

public class GetTask<T extends Storable> extends BaseTask<T, T> {

  private final long key;

  public GetTask(@NotNull Storage<T> storage, long key, Completion<T> completion) {
    super(storage, completion);
    this.key = key;
  }

  @Override
  public T execute() throws Exception {
    return storage.get(key);
  }
}