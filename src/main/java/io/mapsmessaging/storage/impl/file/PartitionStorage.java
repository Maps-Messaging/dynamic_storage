package io.mapsmessaging.storage.impl.file;

import io.mapsmessaging.storage.BaseExpiredHandler;
import io.mapsmessaging.storage.ExpiredMonitor;
import io.mapsmessaging.storage.ExpiredStorableHandler;
import io.mapsmessaging.storage.Statistics;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.StorableFactory;
import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.storage.StorageStatistics;
import io.mapsmessaging.storage.impl.expired.ExpireStorableTaskManager;
import io.mapsmessaging.storage.impl.file.partition.IndexGet;
import io.mapsmessaging.storage.impl.file.partition.IndexRecord;
import io.mapsmessaging.storage.impl.file.partition.IndexStorage;
import io.mapsmessaging.storage.impl.file.tasks.DeletePartitionTask;
import io.mapsmessaging.storage.impl.file.tasks.FileTask;
import io.mapsmessaging.utilities.collections.NaturalOrderedLongList;
import io.mapsmessaging.utilities.collections.NaturalOrderedLongQueue;
import io.mapsmessaging.utilities.collections.bitset.BitSetFactory;
import io.mapsmessaging.utilities.collections.bitset.BitSetFactoryImpl;
import io.mapsmessaging.utilities.threads.tasks.TaskScheduler;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.LongAdder;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class PartitionStorage<T extends Storable> implements Storage<T>, ExpiredMonitor {

  private static final String PARTITION_FILE_NAME = "partition_";

  private final ExpiredStorableHandler expiredHandler;
  private final @Getter
  TaskQueue taskScheduler;
  private final int itemCount;
  private final long maxPartitionSize;
  private final ExpireStorableTaskManager<T> expiredMonitor;

  private final List<IndexStorage<T>> partitions;
  private final String fileName;
  private final String rootDirectory;
  private final StorableFactory<T> storableFactory;
  private final boolean sync;

  private final LongAdder reads;
  private final LongAdder writes;
  private final LongAdder deletes;

  private final LongAdder readTimes;
  private final LongAdder writeTimes;

  private final LongAdder byteWrites;
  private final LongAdder byteReads;

  private boolean shutdown;
  private boolean paused;
  private long partitionCounter;
  private long lastKeyStored;
  private long lastAccess;

  public PartitionStorage(PartitionStorageConfig<T> config) throws IOException{
    partitions = new ArrayList<>();
    taskScheduler = config.getTaskQueue();
    rootDirectory = config.getFileName();
    this.expiredHandler = Objects.requireNonNullElseGet(config.getExpiredHandler(), () -> new BaseExpiredHandler<>(this));
    this.itemCount = config.getItemCount();
    this.maxPartitionSize = config.getMaxPartitionSize();
    this.fileName = config.getFileName() + File.separator + PARTITION_FILE_NAME;
    this.sync = config.isSync();
    this.storableFactory = config.getStorableFactory();
    partitionCounter = 0;
    shutdown = false;
    File location = new File(config.getFileName());
    expiredMonitor = new ExpireStorableTaskManager<>(this, taskScheduler, config.getExpiredEventPoll());
    if (location.exists()) {
      reload(location);
    } else {
      location.mkdir();
      locateOrCreatePartition(0); // Force the creation of the key file
    }
    reads = new LongAdder();
    writes = new LongAdder();
    readTimes = new LongAdder();
    writeTimes = new LongAdder();
    deletes = new LongAdder();
    byteWrites = new LongAdder();
    byteReads = new LongAdder();
    lastKeyStored = -2;
    lastAccess = System.currentTimeMillis();
  }

  @Override
  public String getName() {
    return rootDirectory;
  }

  @Override
  public void shutdown() throws IOException {
    shutdown = true;
    expiredMonitor.close();

    while (taskScheduler.hasTasks()) {
      taskScheduler.executeTasks();
    }
    taskScheduler.abortAll(); // We are about to delete the partition, any tasks can be cancelled now
  }


  @Override
  public void close() throws IOException {
    if (paused) {
      resume(); // need to resume it to set state successfully
    }
    expiredMonitor.close();
    for (IndexStorage<T> partition : partitions) {
      partition.close();
    }
    partitions.clear();
  }

  @Override
  public void delete() throws IOException {
    if (!shutdown) {
      shutdown();
    }
    if (paused) {
      resume();
    }
    for (IndexStorage<T> partition : partitions) {
      partition.delete();
    }
    partitions.clear();
    File file = new File(rootDirectory);
    String[] children = file.list();
    if (children != null && children.length > 0) {
      for (String child : children) {
        File t = new File(child);
        Files.deleteIfExists(t.toPath());
      }
    }
    children = file.list();
    if (children == null || children.length == 0) {
      Files.deleteIfExists(file.toPath());
    }
  }

  @Override
  public boolean supportPause() {
    return true;
  }

  @Override
  public void pause() throws IOException {
    if (!paused) {
      paused = true;
      for (IndexStorage<T> partition : partitions) {
        partition.pause();
      }
      expiredMonitor.pause();
    }
  }

  @Override
  public void resume() throws IOException {
    if (paused) {
      paused = false;
      for (IndexStorage<T> partition : partitions) {
        partition.resume();
      }
      expiredMonitor.resume();
    }

  }

  @Override
  public void add(@NotNull T object) throws IOException {
    if (paused) {
      resume();
    }
    lastAccess = System.currentTimeMillis();
    long time = System.currentTimeMillis();
    IndexStorage<T> partition = locateOrCreatePartition(object.getKey());
    IndexRecord indexRecord = partition.add(object);
    if (partition.isFull()) {
      partition.setEnd(object.getKey());
    }
    expiredMonitor.added(object);
    byteReads.add(IndexRecord.HEADER_SIZE); // We read the header to check for duplicates
    byteWrites.add(indexRecord.getLength());
    writes.increment();
    writeTimes.add((System.currentTimeMillis() - time));
    if (getLastKey() < object.getKey()) {
      lastKeyStored = object.getKey();
    }
  }

  @Override
  public boolean remove(long key) throws IOException {
    if (paused) {
      resume();
    }
    lastAccess = System.currentTimeMillis();

    IndexStorage<T> partition = locatePartition(key);
    if (partition != null) {
      partition.remove(key);
      deletes.increment();
      if (partition.isEmpty() && partitions.size() > 1) {
        partitions.remove(partition);
        submit(new DeletePartitionTask<>(partition));
      }
      byteReads.add(IndexRecord.HEADER_SIZE); // We read it first
      byteWrites.add(IndexRecord.HEADER_SIZE); // We then write a block of zeros
    }
    return false;
  }

  @Override
  public @Nullable T get(long key) throws IOException {
    if (paused) {
      resume();
    }
    lastAccess = System.currentTimeMillis();

    long time = System.currentTimeMillis();
    try {
      IndexStorage<T> partition = locatePartition(key);
      if (partition != null) {
        IndexGet<T> retrieved = partition.get(key);
        if (retrieved != null) {
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
  public @NotNull List<Long> getKeys() {
    List<Long> keyList = new NaturalOrderedLongList();
    for (IndexStorage<T> partition : partitions) {
      keyList.addAll(partition.getKeys());
    }
    return keyList;
  }

  @Override
  public long size() {
    long size = 0;
    for (IndexStorage<T> partition : partitions) {
      size += partition.size();
    }
    return size;
  }

  @Override
  public long getLastKey() {
    if(lastKeyStored == -2){
      lastKeyStored = reloadLastKeyStore();
    }
    return lastKeyStored;
  }

  @Override
  public long getLastAccess() {
    return lastAccess;
  }

  @Override
  public void updateLastAccess() {
    lastAccess = System.currentTimeMillis();
  }

  public long length() throws IOException {
    if (paused) {
      resume();
    }

    long length = 0;
    for (IndexStorage<T> partition : partitions) {
      length += partition.length();
    }
    return length;
  }

  public long emptySpace() {
    long emptySpace = 0;
    for (IndexStorage<T> partition : partitions) {
      emptySpace += partition.emptySpace();
    }
    return emptySpace;
  }

  @Override
  public boolean isEmpty() {
    for (IndexStorage<T> partition : partitions) {
      if (!partition.isEmpty()) {
        return false;
      }
    }
    return true;
  }

  public void scanForExpired() throws IOException {
    if (!paused) {
      try (BitSetFactory bitSetFactory = new BitSetFactoryImpl(8192)) {
        Queue<Long> expiredList = new NaturalOrderedLongQueue(0, bitSetFactory);
        for (IndexStorage<T> partition : partitions) {
          partition.scanForExpired(expiredList);
        }
        if (!expiredList.isEmpty()) {
          expiredHandler.expired(expiredList);
          expiredMonitor.schedulePoll();
        }
      }
    }
  }

  @Override
  public @NotNull List<Long> keepOnly(@NotNull List<Long> listToKeep) {
    for (IndexStorage<T> partition : partitions) {
      listToKeep = partition.keepOnly(listToKeep);
    }
    return listToKeep;
  }

  @Override
  public void setExecutor(TaskScheduler scheduler) {
    taskScheduler.setTaskScheduler(scheduler);
  }

  @Override
  public boolean executeTasks() throws IOException {
    return taskScheduler.executeTasks();
  }

  public @NotNull Statistics getStatistics() {
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

  private long reloadLastKeyStore() {
    if (!partitions.isEmpty()) {
      return (partitions.get(partitions.size() - 1).getLastKey());
    }
    return 0;
  }

  private @Nullable IndexStorage<T> locatePartition(long key) {
    for (IndexStorage<T> partition : partitions) {
      if (partition.getStart() <= key && key <= partition.getEnd()) {
        return partition;
      }
    }
    return null;
  }

  private @NotNull IndexStorage<T> locateOrCreatePartition(long key) throws IOException {
    IndexStorage<T> partition = scanForPartition(key);
    if (partition == null) {
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
    try {
      for (IndexStorage<T> partition : partitions) {
        if (partition.getStart() <= key && key <= partition.getEnd()) {
          return partition;
        }
        if (partition.isEmpty()) {
          empty.add(partition);
        }
      }
    } finally {
      if (!empty.isEmpty()) {
        for (IndexStorage<T> remove : empty) {
          partitions.remove(remove);
          taskScheduler.submit(new DeletePartitionTask<>(remove));
        }
      }
    }
    return null;
  }


  private void reload(File location) throws IOException {
    if (location.isDirectory()) {
      String[] childFiles = location.list();
      if (childFiles != null) {
        boolean hasExpired = false;
        for (String test : childFiles) {
          hasExpired = loadStore(test) || hasExpired;
        }
        if (hasExpired) {
          expiredMonitor.schedulePoll();
        }
      }
    }
    partitions.sort(Comparator.comparingLong(IndexStorage::getStart));
    scanForEmpty();
  }

  private void submit(FileTask<?> task) throws IOException {
    taskScheduler.submit(task);
  }

  private void scanForEmpty() throws IOException {
    List<IndexStorage<T>> emptyReloads = new ArrayList<>();
    for (IndexStorage<T> storage : partitions) {
      if (storage.isEmpty()) {
        emptyReloads.add(storage);
      }
    }
    if (partitions.size() > 1) {
      // OK we have them simply remove them and schedule delete task
      for (IndexStorage<T> storage : emptyReloads) {
        partitions.remove(storage);
        submit(new DeletePartitionTask<>(storage));
        if (partitions.size() == 1) {
          break;
        }
      }
    }
  }

  private boolean loadStore(String test) throws IOException {
    if (test.startsWith(PARTITION_FILE_NAME) && test.endsWith("index")) {
      String loadName = test.substring(PARTITION_FILE_NAME.length(), test.length() - "_index".length());
      IndexStorage<T> indexStorage = new IndexStorage<>(fileName + loadName, storableFactory, sync, 0, itemCount, maxPartitionSize, taskScheduler);
      partitions.add(indexStorage);
      int partNumber = extractPartitionNumber(loadName);
      if (partNumber > partitionCounter) {
        partitionCounter = partNumber;
      }
      return (indexStorage.hasExpired());
    }
    return false;
  }

  private int extractPartitionNumber(String name) {
    return Integer.parseInt(name.trim());
  }

}

