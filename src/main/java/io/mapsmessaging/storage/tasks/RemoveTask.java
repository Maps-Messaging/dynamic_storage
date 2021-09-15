package io.mapsmessaging.storage.tasks;

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.Callable;

public class RemoveTask<T extends Storable> extends BaseTask<T, Boolean>{
  private final long key;
  public RemoveTask(@NotNull Storage<T> storage, long key, Completion<Boolean> completion) {
    super(storage, completion);
    this.key=key;
  }

  @Override
  public Boolean execute() throws Exception {
    return storage.remove(key);
  }
}