package io.mapsmessaging.storage.impl.file.tasks;

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.impl.file.partition.IndexStorage;

public class CompactIndexTask<T extends Storable> implements FileTask<Boolean> {

  private final IndexStorage<T> indexStorage;

  public CompactIndexTask(IndexStorage<T> indexStorage) {
    this.indexStorage = indexStorage;
  }

  @Override
  public Boolean call() throws Exception {
    indexStorage.compact();
    return true;
  }
}
