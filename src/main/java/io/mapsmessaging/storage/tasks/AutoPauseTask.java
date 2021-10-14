package io.mapsmessaging.storage.tasks;

import io.mapsmessaging.storage.AsyncStorage;
import java.io.IOException;

public class AutoPauseTask implements Runnable {

  private final AsyncStorage<?> storage;
  private final long pauseTimeout;

  public AutoPauseTask(AsyncStorage<?> storage, long pauseTimeout){
    this.storage = storage;
    this.pauseTimeout = pauseTimeout;
  }

  @Override
  public void run()  {
    long lastAccess = System.currentTimeMillis() - storage.getLastAccess();
    if(lastAccess > pauseTimeout) {
      try {
        storage.pause();
      } catch (IOException e) {
        // Log required here

      }
    }
  }
}
