package io.mapsmessaging.storage.tasks;

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import org.jetbrains.annotations.NotNull;

public class AddTask<T extends Storable> extends BaseTask<T, T> {

  private final T toStore;

  public AddTask(@NotNull Storage<T> storage, T toStore, Completion<T> completion) {
    super(storage, completion);
    this.toStore = toStore;
  }

  @Override
  public T execute() throws Exception {
    storage.add(toStore);
    return toStore;
  }
}
