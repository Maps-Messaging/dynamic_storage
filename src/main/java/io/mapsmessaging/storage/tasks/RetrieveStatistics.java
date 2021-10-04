package io.mapsmessaging.storage.tasks;

import io.mapsmessaging.storage.Statistics;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import java.util.List;
import org.jetbrains.annotations.NotNull;

public class RetrieveStatistics<T extends Storable> extends BaseTask<T, Statistics> {


  public RetrieveStatistics(@NotNull Storage<T> storage, Completion<Statistics> completion) {
    super(storage, completion);
  }

  @Override
  public Statistics execute() throws Exception {
    return storage.getStatistics();
  }
}
