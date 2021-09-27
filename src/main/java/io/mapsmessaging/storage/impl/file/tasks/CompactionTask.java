package io.mapsmessaging.storage.impl.file.tasks;

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.impl.file.ManagedStorage;

public class CompactionTask<N, T extends Storable> implements FileTask<N>{

  private final ManagedStorage<T> storeToCompact;
  private long index;

  public CompactionTask(ManagedStorage<T> storeToCompact){
    this.storeToCompact = storeToCompact;
    index = 0;
  }

  @Override
  public N call() throws Exception {

    return null;
  }
}
