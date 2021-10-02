package io.mapsmessaging.storage.impl.file;

import io.mapsmessaging.storage.Factory;
import io.mapsmessaging.storage.StorageStatistics;
import io.mapsmessaging.storage.Statistics;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.storage.impl.file.partition.IndexGet;
import io.mapsmessaging.storage.impl.file.partition.IndexRecord;
import io.mapsmessaging.storage.impl.file.partition.IndexStorage;
import io.mapsmessaging.storage.impl.file.tasks.DeletePartitionTask;
import io.mapsmessaging.storage.impl.file.tasks.FileTask;
import io.mapsmessaging.storage.impl.file.tasks.IndexExpiryMonitorTask;
import io.mapsmessaging.utilities.threads.tasks.TaskScheduler;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class PartitionStorage <T extends Storable> implements Storage<T> {

  private static final String PARTITION_FILE_NAME = "partition_";


  private long partitionCounter;
  private final TaskQueue taskScheduler;
  private final int itemCount;
  private final long maxPartitionSize;
  private final List<IndexStorage<T>> partitions;
  private final String fileName;
  private final String rootDirectory;
  private final Factory<T> factory;
  private final boolean sync;
  private boolean shutdown;
  private Future<?> expiryTask;

  private final LongAdder reads;
  private final LongAdder writes;
  private final LongAdder deletes;

  private final LongAdder readTimes;
  private final LongAdder writeTimes;

  private final LongAdder byteWrites;
  private final LongAdder byteReads;


  public PartitionStorage(String fileName, Factory<T> factory, boolean sync, int itemCount, long maxPartitionSize) throws IOException {
    partitions = new ArrayList<>();
    taskScheduler = new TaskQueue();
    rootDirectory = fileName;
    this.itemCount = itemCount;
    this.maxPartitionSize = maxPartitionSize;
    this.fileName = fileName+File.separator+PARTITION_FILE_NAME;
    this.sync = sync;
    this.factory = factory;
    partitionCounter =0;
    shutdown = false;
    File location = new File(fileName);
    expiryTask = null;
    if(location.exists()){
      reload(location);
    }
    else{
      location.mkdir();
    }
    reads = new LongAdder();
    writes = new LongAdder();
    readTimes = new LongAdder();
    writeTimes = new LongAdder();
    deletes = new LongAdder();
    byteWrites = new LongAdder();
    byteReads = new LongAdder();
  }

  private void reload(File location) throws IOException {
    if(location.isDirectory()){
      String[] childFiles = location.list();
      if(childFiles != null) {
        boolean hasExpired = false;
        for (String test :childFiles) {
          if (test.startsWith(PARTITION_FILE_NAME) && test.endsWith("index")){
            String loadName = test.substring(PARTITION_FILE_NAME.length(), test.length()-"_index".length());
            IndexStorage<T> indexStorage = new IndexStorage<>(fileName+loadName, factory, sync, 0, itemCount, maxPartitionSize, taskScheduler);
            partitions.add(indexStorage);
            if(indexStorage.hasExpired()){
              hasExpired = true;
            }
          }
        }
        if(hasExpired){
          expiryTask = taskScheduler.schedule(new IndexExpiryMonitorTask<>(this), 5, TimeUnit.SECONDS);
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
  public void shutdown()throws IOException{
    shutdown = true;
    if(expiryTask != null){
      expiryTask.cancel(true);
      expiryTask = null;
    }

    while(taskScheduler.hasTasks()){
      taskScheduler.executeTasks();
    }
    taskScheduler.abortAll(); // We are about to delete the partition, any tasks can be cancelled now
  }


  @Override
  public void close() throws IOException {
    if(expiryTask != null){
      expiryTask.cancel(true);
      expiryTask = null;
    }

    for(IndexStorage<T> partition:partitions){
      partition.close();
    }
    partitions.clear();
  }

  @Override
  public void delete() throws IOException {
    if(!shutdown){
      shutdown();
    }
    for(IndexStorage<T> partition:partitions){
      partition.delete();
    }
    partitions.clear();
    File file = new File(rootDirectory);
    String[] children = file.list();
    if(children != null && children.length > 0){
      for(String child:children){
        File t = new File(child);
        Files.deleteIfExists(t.toPath());
      }
    }
    children = file.list();
    if(children == null || children.length == 0) {
      Files.deleteIfExists(file.toPath());
    }
  }

  @Override
  public void add(@NotNull T object) throws IOException {
    long time = System.currentTimeMillis();
    IndexStorage<T> partition = locateOrCreatePartition(object.getKey());
    IndexRecord record = partition.add(object);
    if(partition.isFull()){
      partition.setEnd(object.getKey());
    }
    if(object.getExpiry() > 0 && expiryTask == null){
      expiryTask = taskScheduler.schedule(new IndexExpiryMonitorTask<>(this), 5, TimeUnit.SECONDS);
    }
    byteWrites.add(record.getLength());
    writes.increment();
    writeTimes.add((System.currentTimeMillis() - time));
  }

  @Override
  public boolean remove(long key) throws IOException {
    IndexStorage<T> partition = locatePartition(key);
    if(partition != null) {
      partition.remove(key);
      deletes.increment();
      if(partition.isEmpty() && !partitions.isEmpty()){
        partitions.remove(partition);
        submit(new DeletePartitionTask<>( partition));
      }
    }
    return false;
  }

  @Override
  public @Nullable T get(long key) throws IOException {
    long time = System.currentTimeMillis();
    try {
      IndexStorage<T> partition = locatePartition(key);
      if(partition != null) {
        IndexGet<T> retrieved = partition.get(key);
        if(retrieved != null) {
          reads.increment();
          byteReads.add(retrieved.getIndexRecord().getLength());
          return retrieved.getObject();
        }
      }
      return null;
    } finally {
      readTimes.add((System.currentTimeMillis() - time));
    }
  }

  @Override
  public long size() {
    long size =0;
    for(IndexStorage<T> partition:partitions){
      size += partition.size();
    }
    return size;
  }

  public long length() throws IOException {
    long length =0;
    for(IndexStorage<T> partition:partitions){
      length += partition.length();
    }
    return length;
  }

  public long emptySpace(){
    long emptySpace =0;
    for(IndexStorage<T> partition:partitions){
      emptySpace += partition.emptySpace();
    }
    return emptySpace;
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

  public void scanForExpired() throws IOException {
    boolean hasExpired = false;
    List<IndexStorage<T>> toBeRemoved = new ArrayList<>();
    for(IndexStorage<T> partition:partitions){
      if(partition.scanForExpired()){
        hasExpired = true;
      }
      if(partition.isEmpty() && partitions.size() > 1){
        toBeRemoved.add(partition);
      }
    }
    for(IndexStorage<T> partition:toBeRemoved){
      partitions.remove(partition);
      submit(new DeletePartitionTask<>( partition));
    }
    if(hasExpired){
      expiryTask = taskScheduler.schedule(new IndexExpiryMonitorTask<>(this), 5, TimeUnit.SECONDS);
    }
    else{
      expiryTask = null;
    }
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

  private @Nullable IndexStorage<T> locatePartition(long key){
    for(IndexStorage<T> partition:partitions){
      if(partition.getStart() <= key && key <= partition.getEnd()){
        return partition;
      }
    }
    return null;
  }

  private @NotNull IndexStorage<T> locateOrCreatePartition(long key) throws IOException {
    for(IndexStorage<T> partition:partitions){
      if(partition.getStart() <= key && key <= partition.getEnd()){
        return partition;
      }
    }
    String partitionName = fileName+partitionCounter++;
    long start = 0;
    if(!partitions.isEmpty()) {
      start = partitions.get(partitions.size() - 1).getEnd()+1;
    }
    IndexStorage<T> indexStorage = new IndexStorage<>(partitionName, factory, sync, start, itemCount, maxPartitionSize, taskScheduler);
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

  public Statistics getStatistics(){
    long length;
    try {
      length = length();
    } catch (IOException e) {
      length = -1;
    }

    return new StorageStatistics(
        reads.sumThenReset(),
        writes.sumThenReset(),
        deletes.sumThenReset(),
        byteReads.sumThenReset(),
        byteWrites.sumThenReset(),
        readTimes.sumThenReset(),
        writeTimes.sumThenReset(),
        length,
        emptySpace(),
        partitions.size()
    );
  }
}

