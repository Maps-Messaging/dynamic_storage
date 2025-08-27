/*
 *
 *  Copyright [ 2020 - 2024 ] Matthew Buckton
 *  Copyright [ 2024 - 2025 ] MapsMessaging B.V.
 *
 *  Licensed under the Apache License, Version 2.0 with the Commons Clause
 *  (the "License"); you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at:
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *      https://commonsclause.com/
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

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

  public void resume() {
    if (paused) {
      paused = false;
      if (expiryTask != null) {
        schedulePoll();
      }
    }
  }

  @Override
  public void close() throws IOException {
    if (expiryTask != null) {
      expiryTask.cancel(true);
      expiryTask = null;
    }
  }

  public void schedulePoll() {
    if(expiryTask == null || expiryTask.isDone() || expiryTask.isCancelled()) {
      expiryTask = taskScheduler.schedule(new IndexExpiryMonitorTask(storage), poll, TimeUnit.SECONDS);
    }
  }

  public void added(T object) {
    if (object.getExpiry() > 0 && expiryTask == null) {
      schedulePoll();
    }

  }

}
