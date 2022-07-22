package io.mapsmessaging.storage.tasks;

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import java.util.List;
import org.jetbrains.annotations.NotNull;

public class GetKeysTask<T extends Storable> extends BaseTask<T, List<Long>> {

  public GetKeysTask(@NotNull Storage<T> storage, Completion<List<Long>> completion) {
    super(storage, completion);
  }

  @Override
  public List<Long> execute() throws Exception {
    return storage.getKeys();
  }
}