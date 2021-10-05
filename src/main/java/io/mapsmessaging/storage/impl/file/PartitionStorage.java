package io.mapsmessaging.storage.impl.file;

import io.mapsmessaging.storage.StorableFactory;
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
  private final StorableFactory<T> storableFactory;
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


  public PartitionStorage(String fileName, StorableFactory<T> storableFactory, boolean sync, int itemCount, long maxPartitionSize) throws IOException {
    partitions = new ArrayList<>();
    taskScheduler = new TaskQueue();
    rootDirectory = fileName;
    this.itemCount = itemCount;
    this.maxPartitionSize = maxPartitionSize;
    this.fileName = fileName+File.separator+PARTITION_FILE_NAME;
    this.sync = sync;
    this.storableFactory = storableFactory;
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
          hasExpired = loadStore(test) || hasExpired;
        }
        if(hasExpired){
          expiryTask = taskScheduler.schedule(new IndexExpiryMonitorTask<>(this), 5, TimeUnit.SECONDS);
        }
      }
    }
    partitions.sort(Comparator.comparingLong(IndexStorage::getStart));
    List<IndexStorage<T>> emptyReloads = new ArrayList<>();
    for(IndexStorage<T> storage:partitions){
      if(storage.isEmpty()){
        emptyReloads.add(storage);
      }
    }
    // OK we have them simply remove them and schedule delete task
    for(IndexStorage<T> storage:emptyReloads){
      partitions.remove(storage);
      submit(new DeletePartitionTask<>(storage));
    }
  }

  private boolean loadStore(String test) throws IOException {
    if (test.startsWith(PARTITION_FILE_NAME) && test.endsWith("index")){
      String loadName = test.substring(PARTITION_FILE_NAME.length(), test.length()-"_index".length());
      IndexStorage<T> indexStorage = new IndexStorage<>(fileName+loadName, storableFactory, sync, 0, itemCount, maxPartitionSize, taskScheduler);
      partitions.add(indexStorage);
      int partNumber = extractPartitionNumber(loadName);
      if(partNumber > partitionCounter){
        partitionCounter = partNumber;
      }
      return(indexStorage.hasExpired());
    }
    return false;
  }

  private int extractPartitionNumber(String name){
    return Integer.parseInt(name.trim());
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
    IndexRecord indexRecord = partition.add(object);
    if(partition.isFull()){
      partition.setEnd(object.getKey());
    }
    if(object.getExpiry() > 0 && expiryTask == null){
      expiryTask = taskScheduler.schedule(new IndexExpiryMonitorTask<>(this), 5, TimeUnit.SECONDS);
    }
    byteReads.add(IndexRecord.HEADER_SIZE); // We read the header to check for duplicates
    byteWrites.add(indexRecord.getLength());
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
      byteReads.add(IndexRecord.HEADER_SIZE); // We read it first
      byteWrites.add(IndexRecord.HEADER_SIZE); // We then write a block of zeros
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
    IndexStorage<T> partition = scanForPartition(key);
    if(partition == null) {
      String partitionName = fileName + partitionCounter++;
      long start = 0;
      if (!partitions.isEmpty()) {
        start = partitions.get(partitions.size() - 1).getEnd() + 1;
      }
      if (key < start || key > (start + itemCount)) {
        start = key;
      }
      partition = new IndexStorage<>(partitionName, storableFactory, sync, start, itemCount, maxPartitionSize, taskScheduler);
      partitions.add(partition);
      partitions.sort(Comparator.comparingLong(IndexStorage::getStart));
    }
    return partition;
  }

  private IndexStorage<T> scanForPartition(long key) throws IOException {
    List<IndexStorage<T>> empty = new ArrayList<>();
    for(IndexStorage<T> partition:partitions){
      if(partition.getStart() <= key && key <= partition.getEnd()){
        if(!empty.isEmpty()){
          for(IndexStorage<T> remove:empty){
            partitions.remove(remove);
            taskScheduler.submit(new DeletePartitionTask<>(remove));
          }
        }
        return partition;
      }
      if(partition.isEmpty()){
        empty.add(partition);
      }
    }
    if(!empty.isEmpty()){
      for(IndexStorage<T> remove:empty){
        partitions.remove(remove);
        taskScheduler.submit(new DeletePartitionTask<>(remove));
      }
    }
    return null;
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

