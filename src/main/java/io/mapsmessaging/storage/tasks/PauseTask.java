package io.mapsmessaging.storage.tasks;

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import org.jetbrains.annotations.NotNull;

public class PauseTask <T extends Storable> extends BaseTask<T, Void> {

  public PauseTask(@NotNull Storage<T> storage) {
    super(storage, null);
  }

  @Override
  public Void execute() throws Exception {
    storage.pause();
    return null;
  }

}