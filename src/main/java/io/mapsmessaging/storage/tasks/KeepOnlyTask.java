package io.mapsmessaging.storage.tasks;

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.concurrent.Callable;

public class KeepOnlyTask<T extends Storable> extends BaseTask<T, List<Long>>{
  private final List<Long> toKeep;

  public KeepOnlyTask(@NotNull Storage<T> storage, List<Long> toKeep, Completion<List<Long>> completion) {
    super(storage, completion);
    this.toKeep = toKeep;
  }

  @Override
  public List<Long> execute() throws Exception {
    return storage.keepOnly(toKeep);
  }
}