package io.mapsmessaging.storage.impl.expired;

import io.mapsmessaging.storage.ExpiredMonitor;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.impl.file.TaskQueue;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ExpireStorableTaskManager<T extends Storable> implements Closeable {

  private final ExpiredMonitor storage;
  private final TaskQueue taskScheduler;
  private final int poll;
  private Future<?> expiryTask;

  public ExpireStorableTaskManager(ExpiredMonitor storage, TaskQueue taskScheduler, int poll){
    this.storage = storage;
    this.taskScheduler = taskScheduler;
    this.poll = poll;
    expiryTask = null;
  }

  @Override
  public void close() throws IOException {
    if(expiryTask != null){
      expiryTask.cancel(true);
      expiryTask = null;
    }
  }

  public void schedulePoll(){
    expiryTask = taskScheduler.schedule(new IndexExpiryMonitorTask(storage), poll, TimeUnit.SECONDS);
  }

  public void added(T object){
    if(object.getExpiry() > 0 && expiryTask == null){
      schedulePoll();
    }

  }

}
