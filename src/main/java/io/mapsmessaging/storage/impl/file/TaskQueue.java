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
import lombok.ToString;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

@ToString
public class TaskQueue {

  private static final long TIMEOUT = 60;
  private static final ScheduledThreadPoolExecutor SCHEDULER_EXECUTOR =
      (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());

  static {
    Runtime.getRuntime().addShutdownHook(new ShutdownHandler());
  }

  private final Queue<FileTask<?>> syncTasks;
  private final AtomicLong waitingScheduler;
  private final Map<FileTask<?>, Future<?>> pending;

  private ExecutorService taskScheduler;

  public static void shutdown() {
    SCHEDULER_EXECUTOR.shutdown();
  }

  public TaskQueue() {
    syncTasks = new LinkedList<>();
    pending = new ConcurrentHashMap<>();
    waitingScheduler = new AtomicLong(0);
  }

  public void abortAll() throws IOException {
    long timeout = System.currentTimeMillis() + 1000;
    while (waitingScheduler.get() != 0 && timeout > System.currentTimeMillis()) {
      LockSupport.parkNanos(10000000);
    }
    syncTasks.clear();
    clearQueue();
  }

  private void clearQueue() throws IOException {
    IOException raised = null;
    for (Map.Entry<FileTask<?>, Future<?>> entry : pending.entrySet()) {
      try {
        processOutstandingTask(entry.getKey(), entry.getValue());
      } catch (IOException e) {
        raised = e;
      }
    }
    pending.clear();
    if (raised != null) {
      throw raised;
    }
  }

  private void processOutstandingTask(FileTask<?> task, Future<?> future) throws IOException {
    if (!future.isDone()) {
      if (task.canCancel()) {
        future.cancel(true);
      } else {
        try {
          future.get(TIMEOUT, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException(e);
        } catch (ExecutionException | TimeoutException e) {
          throw new IOException(e);
        }
      }
    }
  }

  public boolean isEmpty() {
    return pending.isEmpty();
  }

  public void setTaskScheduler(@NotNull ExecutorService scheduler) {
    taskScheduler = scheduler;
    while (!syncTasks.isEmpty()) {
      FileTask<?> task = syncTasks.poll();
      if (task != null) {
        Future<?> future = taskScheduler.submit(task);
        trackPending(task, future);
      }
    }
  }

  public <V> Future<V> scheduleNow(FileTask<V> raw) {
    FileWrapperTask<V> task = new FileWrapperTask<>(raw, pending);
    Future<V> future = SCHEDULER_EXECUTOR.submit(task);
    trackPending(task, future);
    return future;
  }

  public <V> Future<V> schedule(FileTask<V> raw, long startIn, TimeUnit timeUnit) {
    FileWrapperTask<V> task = new FileWrapperTask<>(raw, pending);
    ScheduledFuture<V> future = SCHEDULER_EXECUTOR.schedule(task, startIn, timeUnit);
    trackPending(task, future);
    return future;
  }

  @SuppressWarnings("java:S1452")
  public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long startIn, TimeUnit timeUnit) {
    return SCHEDULER_EXECUTOR.scheduleAtFixedRate(command, startIn, startIn, timeUnit);
  }

  public void submit(FileTask<?> raw) throws IOException {
    submitInternalTask(raw);
  }

  private void submitInternalTask(FileTask<?> raw) throws IOException {
    FileWrapperTask<?> task = new FileWrapperTask<>(raw, pending);
    if (taskScheduler != null) {
      waitingScheduler.incrementAndGet();
      SubmitTask submitTask = new SubmitTask(task);
      SCHEDULER_EXECUTOR.submit(submitTask);
    } else {
      syncTasks.offer(task);
      if (syncTasks.size() > 10) {
        while (!syncTasks.isEmpty()) {
          executeTasks();
        }
      }
    }
  }

  @SuppressWarnings("java:S112")
  public boolean executeTasks() throws IOException {
    FileTask<?> task = syncTasks.poll();
    if (task != null) {
      try {
        task.call();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
    return hasTasks();
  }

  public boolean hasTasks() {
    return !syncTasks.isEmpty();
  }

  public void purge() {
    SCHEDULER_EXECUTOR.purge();
  }

  private void trackPending(FileTask<?> task, Future<?> future) {
    if (!future.isDone()) {
      pending.put(task, future);
      if (future.isDone()) {
        pending.remove(task);
      }
    }
  }

  private static final class FileWrapperTask<T> implements FileTask<T> {

    private final FileTask<T> task;
    private final Map<FileTask<?>, Future<?>> pending;

    public FileWrapperTask(FileTask<T> task, Map<FileTask<?>, Future<?>> pending) {
      this.task = task;
      this.pending = pending;
    }

    @Override
    public boolean canCancel() {
      return task.canCancel();
    }

    @Override
    public T call() throws Exception {
      try {
        return task.call();
      } finally {
        pending.remove(this);
      }
    }
  }

  private final class SubmitTask implements FileTask<Boolean> {

    private final FileTask<?> toSubmit;

    public SubmitTask(FileTask<?> toSubmit) {
      this.toSubmit = toSubmit;
    }

    @Override
    public Boolean call() {
      try {
        Future<?> future = taskScheduler.submit(toSubmit);
        trackPending(toSubmit, future);
        return true;
      } finally {
        waitingScheduler.decrementAndGet();
      }
    }
  }

  public static final class ShutdownHandler extends Thread {

    @Override
    public void run() {
      TaskQueue.shutdown();
    }
  }
}