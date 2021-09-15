package io.mapsmessaging.storage.tasks;

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.Callable;

public class DeleteTask <T extends Storable> extends BaseTask<T, Boolean>{
  public DeleteTask(@NotNull Storage<T> storage, Completion<Boolean> callable) {
    super(storage, callable);
  }

  @Override
  public Boolean execute() throws Exception {
    storage.delete();
    return true;
  }
}