package io.mapsmessaging.storage.tasks;

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import org.jetbrains.annotations.NotNull;

public class ContainsTask<T extends Storable> extends BaseTask<T, Boolean> {

  private final long key;

  public ContainsTask(@NotNull Storage<T> storage, long key, Completion<Boolean> callable) {
    super(storage, callable);
    this.key = key;
  }

  @Override
  public Boolean execute() throws Exception {
    return storage.contains(key);
  }
}