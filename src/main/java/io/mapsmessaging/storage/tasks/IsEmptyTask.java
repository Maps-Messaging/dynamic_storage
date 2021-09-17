package io.mapsmessaging.storage.tasks;

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import org.jetbrains.annotations.NotNull;

public class IsEmptyTask<T extends Storable> extends BaseTask<T, Boolean> {

  public IsEmptyTask(@NotNull Storage<T> storage) {
    super(storage, null);
  }

  @Override
  public Boolean execute() throws Exception {
    return storage.isEmpty();
  }
}