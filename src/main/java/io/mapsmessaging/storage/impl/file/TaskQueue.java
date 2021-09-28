package io.mapsmessaging.storage.impl.file;

import io.mapsmessaging.storage.impl.file.tasks.FileTask;
import io.mapsmessaging.utilities.threads.tasks.TaskScheduler;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import org.jetbrains.annotations.NotNull;

public class TaskQueue {

  private TaskScheduler taskScheduler;
  private final Queue<FileTask<?>> taskQueue;

  public TaskQueue(){
    taskQueue = new LinkedList<>();
  }

  public void setTaskScheduler(@NotNull TaskScheduler scheduler){
    taskScheduler = scheduler;
    while(!taskQueue.isEmpty()){
      taskScheduler.submit(taskQueue.poll());
    }
  }

  public void submit(FileTask<?> task) throws IOException {
    if(taskScheduler != null) {
      Thread t = new Thread(() -> taskScheduler.submit(task));
      t.start();
    }
    else{
      taskQueue.offer(task);
      if(taskQueue.size() > 10){
        while(!taskQueue.isEmpty()){
          executeTasks();
        }
      }
    }
  }

  public boolean executeTasks() throws IOException {
    FileTask<?> task = taskQueue.poll();
    if(task != null){
      try {
        task.call();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
    return !taskQueue.isEmpty();
  }

}
