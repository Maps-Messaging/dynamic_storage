package io.mapsmessaging.storage.tasks;

import io.mapsmessaging.storage.Statistics;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import org.jetbrains.annotations.NotNull;

public class RetrieveStatisticsTask<T extends Storable> extends BaseTask<T, Statistics> {


  public RetrieveStatisticsTask(@NotNull Storage<T> storage, Completion<Statistics> completion) {
    super(storage, completion);
  }

  @Override
  public Statistics execute() throws Exception {
    return storage.getStatistics();
  }
}
