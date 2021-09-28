package io.mapsmessaging.storage.impl.file;

import io.mapsmessaging.storage.Factory;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.storage.impl.file.partition.IndexStorage;
import io.mapsmessaging.storage.impl.file.tasks.DeletePartitionTask;
import io.mapsmessaging.storage.impl.file.tasks.FileTask;
import io.mapsmessaging.utilities.threads.tasks.TaskScheduler;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class PartitionStorage <T extends Storable> implements Storage<T> {

  private long partitionCounter;
  private final TaskQueue taskScheduler;
  private final List<IndexStorage<T>> partitions;
  private final String fileName;
  private final String rootDirectory;
  private final Factory<T> factory;
  private final boolean sync;

  public PartitionStorage(String fileName, Factory<T> factory, boolean sync) throws IOException {
    partitions = new ArrayList<>();
    taskScheduler = new TaskQueue();
    rootDirectory = fileName;
    this.fileName = fileName+File.separator+"partition_";
    this.sync = sync;
    this.factory = factory;
    partitionCounter =0;
    File location = new File(fileName);
    if(location.exists()){
      reload(location);
    }
    else{
      location.mkdir();
    }
  }

  private void reload(File location) throws IOException {
    if(location.isDirectory()){
      String[] childFiles = location.list();
      if(childFiles != null) {
        for (String test :childFiles) {
          if (test.startsWith("partition_") && test.endsWith("index")){
            String loadName = test.substring("partition_".length(), test.length()-"_index".length());
            IndexStorage<T> indexStorage = new IndexStorage<>(fileName+loadName, factory, sync, 0, taskScheduler);
            partitions.add(indexStorage);
          }
        }
      }
    }
    partitions.sort(Comparator.comparingLong(IndexStorage::getStart));
  }

  @Override
  public String getName() {
    return rootDirectory;
  }

  @Override
  public void delete() throws IOException {
    for(IndexStorage<T> partition:partitions){
      partition.delete();
    }
    partitions.clear();
    File file = new File(rootDirectory);
    Files.deleteIfExists(file.toPath());
  }

  @Override
  public void add(@NotNull T object) throws IOException {
    IndexStorage<T> partition = locateOrCreatePartition(object.getKey());
    partition.add(object);
  }

  @Override
  public boolean remove(long key) throws IOException {
    IndexStorage<T> partition = locatePartition(key);
    if(partition != null) {
      partition.remove(key);
      if(partition.isEmpty() && !partitions.isEmpty()){
        partitions.remove(partition);
        submit(new DeletePartitionTask<T>( partition));
      }
    }
    return false;
  }

  @Override
  public @Nullable T get(long key) throws IOException {
    IndexStorage<T> partition = locatePartition(key);
    if(partition != null) {
      return partition.get(key);
    }
    return null;
  }

  @Override
  public long size() throws IOException {
    long size =0;
    for(IndexStorage<T> partition:partitions){
      size += partition.size();
    }
    return size;
  }

  @Override
  public boolean isEmpty() {
    for(IndexStorage<T> partition:partitions){
      if(!partition.isEmpty()){
        return false;
      }
    }
    return true;
  }

  @Override
  public @NotNull List<Long> keepOnly(@NotNull List<Long> listToKeep) throws IOException {
    for(IndexStorage<T> partition:partitions){
      listToKeep = partition.keepOnly(listToKeep);
    }
    return listToKeep;
  }

  @Override
  public void setExecutor(TaskScheduler scheduler) {
    taskScheduler.setTaskScheduler(scheduler);
  }

  @Override
  public void close() throws IOException {
    for(IndexStorage<T> partition:partitions){
      partition.close();
    }
    partitions.clear();
  }

  private @Nullable IndexStorage<T> locatePartition(long key){
    for(IndexStorage<T> partition:partitions){
      if(partition.getStart() <= key && key < partition.getEnd()){
        return partition;
      }
    }
    return null;
  }

  private @NotNull IndexStorage<T> locateOrCreatePartition(long key) throws IOException {
    for(IndexStorage<T> partition:partitions){
      if(partition.getStart() <= key && key < partition.getEnd()){
        return partition;
      }
    }
    String partitionName = fileName+partitionCounter++;
    long start = 0;
    if(!partitions.isEmpty()) {
      start = partitions.get(partitions.size() - 1).getEnd();
    }
    IndexStorage<T> indexStorage = new IndexStorage<>(partitionName, factory, sync, start, taskScheduler);
    partitions.add(indexStorage);
    partitions.sort(Comparator.comparingLong(IndexStorage::getStart));
    return indexStorage;
  }

  private void submit(FileTask<?> task) throws IOException {
    taskScheduler.submit(task);
  }

  @Override
  public boolean executeTasks() throws IOException {
    return taskScheduler.executeTasks();
  }
}
