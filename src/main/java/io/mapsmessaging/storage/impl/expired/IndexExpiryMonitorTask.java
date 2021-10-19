package io.mapsmessaging.storage.impl.expired;

import io.mapsmessaging.storage.ExpiredMonitor;
import io.mapsmessaging.storage.impl.file.tasks.FileTask;
import java.io.IOException;

public class IndexExpiryMonitorTask implements FileTask<Boolean> {

  private final ExpiredMonitor storage;

  public IndexExpiryMonitorTask(ExpiredMonitor storage) {
    this.storage = storage;
  }

  @Override
  public Boolean call() throws IOException {
    storage.scanForExpired();
    return true;
  }

  @Override
  public boolean canCancel() {
    return true;
  }

}
