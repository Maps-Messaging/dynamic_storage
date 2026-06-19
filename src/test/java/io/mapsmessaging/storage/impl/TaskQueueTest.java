/*
 *
 *  Copyright [ 2020 - 2024 ] Matthew Buckton
 *  Copyright [ 2024 - 2026 ] MapsMessaging B.V.
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

package io.mapsmessaging.storage.impl.file;

import io.mapsmessaging.storage.impl.file.tasks.FileTask;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

class TaskQueueTest {

  @Test
  void synchronousTaskExecutionDoesNotLeaveWaitingSchedulerCount() throws Exception {
    TaskQueue taskQueue = new TaskQueue();
    AtomicInteger calls = new AtomicInteger();

    taskQueue.submit(new CountingFileTask(calls));

    Assertions.assertTrue(taskQueue.hasTasks());
    Assertions.assertFalse(taskQueue.executeTasks());
    Assertions.assertEquals(1, calls.get());
    Assertions.assertEquals(0, waitingSchedulerCount(taskQueue));

    long start = System.nanoTime();
    taskQueue.abortAll();
    long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

    Assertions.assertTrue(elapsedMillis < 250, "abortAll should not wait for stale scheduler handoff state");
  }

  private long waitingSchedulerCount(TaskQueue taskQueue) throws Exception {
    Field field = TaskQueue.class.getDeclaredField("waitingScheduler");
    field.setAccessible(true);
    AtomicLong waitingScheduler = (AtomicLong) field.get(taskQueue);
    return waitingScheduler.get();
  }

  private static final class CountingFileTask implements FileTask<Boolean> {

    private final AtomicInteger calls;

    private CountingFileTask(AtomicInteger calls) {
      this.calls = calls;
    }

    @Override
    public boolean canCancel() {
      return true;
    }

    @Override
    public Boolean call() {
      calls.incrementAndGet();
      return true;
    }
  }
}