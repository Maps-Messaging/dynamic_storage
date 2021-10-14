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
  private boolean paused;

  public ExpireStorableTaskManager(ExpiredMonitor storage, TaskQueue taskScheduler, int poll) {
    this.storage = storage;
    this.taskScheduler = taskScheduler;
    this.poll = poll;
    paused = false;
    expiryTask = null;
  }

  public void pause() {
    if (!paused) {
      paused = true;
      if (expiryTask != null) {
        expiryTask.cancel(false);
      }
    }
  }

  public void resume(){
    if (paused) {
      paused = false;
      if (expiryTask != null) {
        schedulePoll();
      }
    }
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
