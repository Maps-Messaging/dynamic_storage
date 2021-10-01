package io.mapsmessaging.storage.impl.file.tasks;

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.impl.file.PartitionStorage;
import java.io.IOException;

public class IndexExpiryMonitorTask<T extends Storable> implements FileTask<Boolean> {

  private final PartitionStorage<T> storage;

  public IndexExpiryMonitorTask(PartitionStorage<T> storage) {
    this.storage = storage;
  }

  @Override
  public Boolean call() throws IOException {
    storage.scanForExpired();
    return true;
  }

  @Override
  public boolean canCancel(){
    return true;
  }

  @Override
  public boolean independentTask(){
    return false;
  }
}
