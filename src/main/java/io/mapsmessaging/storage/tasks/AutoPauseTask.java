/*
 *   Copyright [2020 - 2022]   [Matthew Buckton]
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 */

package io.mapsmessaging.storage.tasks;

import io.mapsmessaging.storage.AsyncStorage;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class AutoPauseTask implements Runnable {

  private final AsyncStorage<?> storage;
  private final long pauseTimeout;

  public AutoPauseTask(AsyncStorage<?> storage, long pauseTimeout) {
    this.storage = storage;
    this.pauseTimeout = pauseTimeout;
  }

  @Override
  public void run() {
    try {
      Future<Long> future = storage.getLastAccessAsync();
      long lastAccess = System.currentTimeMillis() - future.get(1000, TimeUnit.MILLISECONDS);
      if (lastAccess > pauseTimeout) {
        storage.pause();
      }
    }
    catch (IOException| ExecutionException | TimeoutException e) {
      Thread.currentThread().interrupt();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
