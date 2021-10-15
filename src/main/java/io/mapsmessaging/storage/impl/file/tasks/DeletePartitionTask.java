package io.mapsmessaging.storage.impl.file.tasks;

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.impl.file.partition.IndexStorage;

public class DeletePartitionTask<T extends Storable> implements FileTask<Boolean> {

  private final IndexStorage<T> partitionToDelete;

  public DeletePartitionTask(IndexStorage<T> partitionToDelete) {
    this.partitionToDelete = partitionToDelete;
  }

  @Override
  public Boolean call() throws Exception {
    partitionToDelete.delete();
    return true;
  }

  @Override
  public boolean canCancel() {
    return false;
  }

  @Override
  public boolean independentTask() {
    return true;
  }
}
