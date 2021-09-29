package io.mapsmessaging.storage.tasks;

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import org.jetbrains.annotations.NotNull;

public class ClearScheduleTask <T extends Storable> extends BaseTask<T, Boolean> {

  public ClearScheduleTask(@NotNull Storage<T> storage, Completion<Boolean> callable) {
    super(storage, callable);
  }

  @Override
  public Boolean execute() throws Exception {
    return true;
  }
}