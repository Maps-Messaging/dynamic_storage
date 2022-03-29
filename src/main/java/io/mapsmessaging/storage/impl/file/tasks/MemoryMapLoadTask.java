package io.mapsmessaging.storage.impl.file.tasks;

import io.mapsmessaging.storage.impl.file.partition.IndexManager;

public class MemoryMapLoadTask  implements FileTask<Boolean> {

  private final IndexManager indexStorage;
  private final boolean walkIndex;

  public MemoryMapLoadTask(IndexManager indexStorage, boolean walkIndex) {
    this.indexStorage = indexStorage;
    this.walkIndex = walkIndex;
  }

  @Override
  public boolean canCancel() {
    return false;
  }

  @Override
  public Boolean call() {
    indexStorage.loadMap(walkIndex);
    return true;
  }
}
