package io.mapsmessaging.storage.impl.file;

import io.mapsmessaging.storage.impl.file.tasks.FileTask;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.jetbrains.annotations.NotNull;

public class TaskQueue {

  private static final long TIMEOUT = 60;
  private static final ScheduledExecutorService SCHEDULER_EXECUTOR = Executors.newScheduledThreadPool(2);
  private static final ExecutorService INDEPENDENT_EXECUTOR = Executors.newSingleThreadExecutor();

  private final Queue<FileTask<?>> syncTasks;
  private final AtomicLong waitingScheduler;
  private final Map<FileTask<?>,Future<?>> pending;

  private ExecutorService taskScheduler;

  public TaskQueue(){
    syncTasks = new LinkedList<>();
    pending = new ConcurrentHashMap<>();
    waitingScheduler = new AtomicLong(0);
  }

  public void abortAll() throws IOException {
    long timeout = System.currentTimeMillis() + 1000;
    while(waitingScheduler.get() != 0 && timeout>System.currentTimeMillis()){
      LockSupport.parkNanos(10000000);
    }
    syncTasks.clear();
    clearQueue();
  }

  private void clearQueue() throws IOException {
    IOException raised = null;
    for(Map.Entry<FileTask<?>,Future<?>> entry: pending.entrySet()){
      try {
        processOutstandingTask(entry.getKey(), entry.getValue());
      } catch (IOException e) {
        raised = e;
      }
    }
    pending.clear();
    if(raised != null){
      throw raised;
    }
  }

  private void processOutstandingTask(FileTask<?> task,Future<?> future ) throws IOException {
    if(!future.isDone()) {
      if (task.canCancel()) {
        future.cancel(true);
      } else {
        try {
          future.get(TIMEOUT, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
          if (Thread.interrupted()) {
            Thread.currentThread().interrupt();
          }
          throw new IOException(e);
        }
      }
    }
  }
  public boolean isEmpty(){
    return pending.isEmpty();
  }

  public void setTaskScheduler(@NotNull ExecutorService scheduler){
    taskScheduler = scheduler;
    while(!syncTasks.isEmpty()){
      taskScheduler.submit(syncTasks.poll());
    }
  }

  public Future<?> schedule(FileTask<?> raw, long startIn, TimeUnit timeUnit){
    FileWrapperTask<?> task = new FileWrapperTask<>(raw, pending);
    return SCHEDULER_EXECUTOR.schedule (task, startIn, timeUnit);
  }

  public Future<?> scheduleAtFixedRate(Runnable raw, long startIn, TimeUnit timeUnit){
    return SCHEDULER_EXECUTOR.scheduleAtFixedRate (raw, startIn, startIn, timeUnit);
  }

  public void submit(FileTask<?> raw) throws IOException {
    if(raw.independentTask()){
      INDEPENDENT_EXECUTOR.submit(raw);
    }
    else {
      waitingScheduler.incrementAndGet();
      FileWrapperTask<?> task = new FileWrapperTask<>(raw, pending);
      if (taskScheduler != null) {
        SubmitTask submitTask = new SubmitTask(task);
        INDEPENDENT_EXECUTOR.submit(submitTask);
      } else {
        syncTasks.offer(task);
        if (syncTasks.size() > 10) {
          while (!syncTasks.isEmpty()) {
            executeTasks();
          }
        }
      }
    }
  }

  @SuppressWarnings("java:S112")
  public boolean executeTasks() throws IOException {
    FileTask<?> task = syncTasks.poll();
    if(task != null){
      try {
        task.call();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
    return hasTasks();
  }

  public boolean hasTasks(){
    return !syncTasks.isEmpty();
  }

  private static final class FileWrapperTask<T> implements FileTask<T>{

    private final FileTask<T> task;
    private final Map<FileTask<?>,Future<?>> pending;

    public FileWrapperTask(FileTask<T> task,Map<FileTask<?>,Future<?>> pending){
      this.task = task;
      this.pending = pending;
    }

    @Override
    public boolean canCancel(){
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

  private final class SubmitTask implements FileTask<Boolean>{

    private final FileTask<?> toSubmit;
    public SubmitTask(FileTask<?> toSubmit){
      this.toSubmit = toSubmit;
    }

    @Override
    public Boolean call()  {
      pending.put(toSubmit, taskScheduler.submit(toSubmit));
      waitingScheduler.decrementAndGet();
      return true;
    }
  }

}
