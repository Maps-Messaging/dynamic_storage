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

package io.mapsmessaging.storage.impl.file.partition;

import static io.mapsmessaging.storage.impl.file.partition.PartitionDataManagerFactory.getInstance;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.DSYNC;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.SPARSE;
import static java.nio.file.StandardOpenOption.WRITE;

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.StorableFactory;
import io.mapsmessaging.storage.impl.file.TaskQueue;
import io.mapsmessaging.storage.impl.file.tasks.CompactIndexTask;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class IndexStorage<T extends Storable> {

  private static final int HEADER_SIZE = 32;

  private static final double VERSION = 1.0;
  private static final long UNIQUE_ID = 0xf00d0000d00f0000L;
  private static final long OPEN_STATE = 0xEFFFFFFFFFFFFFFFL;
  private static final long CLOSE_STATE = 0x0000000000000000L;

  private int itemCount;
  private final boolean sync;
  private final String fileName;
  private final TaskQueue scheduler;
  private final ArchivedDataStorage<T> dataStorage;

  private IndexManager indexManager;
  private FileChannel mapChannel;

  private volatile boolean closed;
  private volatile boolean paused;
  private boolean requiresValidation;

  public IndexStorage(String name, StorableFactory<T> storableFactory, boolean sync, long start, int itemCount, long maxPartitionSize, TaskQueue taskScheduler) throws IOException {
    this.fileName = name + "_index";
    File file = new File(this.fileName);
    scheduler = taskScheduler;
    this.sync = sync;
    this.itemCount = itemCount;
    long length = 0;
    if (file.exists()) {
      length = file.length();
    }
    mapChannel = openChannel(file);
    if (length != 0) {
      indexManager = reload();
    } else {
      indexManager = initialise(start);
    }
    ArchivedDataStorage store = getInstance().create(null, fileName+ "_data", storableFactory, sync, maxPartitionSize);
    dataStorage = (ArchivedDataStorage<T>) store;
    if (dataStorage.isValidationRequired() || requiresValidation) {
      // We need to validate the data / index

    }
    closed = false;
    paused = false;
  }

  public void close() throws IOException {
    if (!closed) {
      closed = true;
      ByteBuffer header = ByteBuffer.allocate(8);
      header.putLong(CLOSE_STATE);
      header.flip();
      mapChannel.position(0);
      mapChannel.write(header);
      indexManager.close();
      mapChannel.force(true);
      mapChannel.close();
      dataStorage.close();
    }
  }

  public void delete() throws IOException {
    indexManager.close();
    mapChannel.close();
    dataStorage.delete();
    File path = new File(fileName);
    Files.delete(path.toPath());
  }

  public void pause() throws IOException {
    if (!paused) {
      paused = true;
      indexManager.pause();
      mapChannel.force(true);

      mapChannel.close();
      dataStorage.pause();
    }
  }

  public void resume() throws IOException {
    if (paused) {
      paused = false;
      File file = new File(this.fileName);
      mapChannel = openChannel(file);
      indexManager.resume(mapChannel);
      dataStorage.resume();
    }
  }

  private IndexManager initialise(long start) throws IOException {
    ByteBuffer headerValidation = ByteBuffer.allocate(HEADER_SIZE);
    headerValidation.putLong(OPEN_STATE);
    headerValidation.putLong(UNIQUE_ID);
    headerValidation.putLong(Double.doubleToLongBits(VERSION));
    headerValidation.putLong(itemCount);
    headerValidation.flip();
    mapChannel.write(headerValidation);
    IndexManager idx = new IndexManager(start, itemCount, mapChannel);
    scheduler.scheduleNow(idx.queueTask(false));
    mapChannel.force(false);
    requiresValidation = false;
    return idx;
  }

  private IndexManager reload() throws IOException {
    ByteBuffer headerValidation = ByteBuffer.allocate(HEADER_SIZE);
    mapChannel.read(headerValidation);
    headerValidation.flip();
    requiresValidation = headerValidation.getLong() != CLOSE_STATE;
    if (headerValidation.getLong() != UNIQUE_ID) {
      throw new IOException("Unexpected file identifier located");
    }
    if (Double.longBitsToDouble(headerValidation.getLong()) != VERSION) {
      throw new IOException("Unexpected file version");
    }
    if (headerValidation.getLong() != itemCount) {
      itemCount = (int)(headerValidation.getLong() & 0x7fffffffL);
    }
    IndexManager idx = new IndexManager(mapChannel);
    idx.loadMap(true);
    headerValidation.flip();
    headerValidation.putLong(0, OPEN_STATE);
    mapChannel.position(0);
    mapChannel.write(headerValidation);
    mapChannel.force(false);
    return idx;
  }

  public void compact() throws IOException {
    long size = ((indexManager.getEnd() - indexManager.getStart() + 2) * IndexRecord.HEADER_SIZE) + 24 + 16;
    long mapSize = mapChannel.size();
    if (size <mapSize) {
      File currentIndex = new File(fileName);
      File tmpIndex = new File(fileName + "_tmp");
      try (FileChannel tmp = (FileChannel) Files.newByteChannel(tmpIndex.toPath(), CREATE_NEW, WRITE)) {
        mapChannel.position(0);
        long moved = tmp.transferFrom(mapChannel, 0, size);
        if (moved != size) {
          Files.deleteIfExists(tmpIndex.toPath());
          throw new IOException("Unable to compact index");
        }
        tmp.force(true);
      }
      indexManager.close();
      mapChannel.force(true);
      mapChannel.close();

      Files.copy(tmpIndex.toPath(), currentIndex.toPath(), StandardCopyOption.REPLACE_EXISTING);
      Files.deleteIfExists(tmpIndex.toPath());
      mapChannel = openChannel(currentIndex);
      indexManager = reload();
    }
  }

  public boolean hasExpired() {
    return !indexManager.getExpiryIndex().isEmpty();
  }

  public List<Long> getKeys() {
    return indexManager.keySet();
  }

  public long getLastKey() {
    return getStart() + indexManager.getMaxKey();
  }

  public void scanForExpired(Queue<Long> expiredList) {
    indexManager.scanForExpired(expiredList);
  }

  public long getStart() {
    return indexManager.getStart();
  }

  public long getEnd() {
    return indexManager.getEnd();
  }

  public void setEnd(long key) throws IOException {
    indexManager.setEnd(key);
    scheduler.submit(new CompactIndexTask<>(this));
  }


  public String getName() {
    return fileName;
  }

  public IndexRecord add(@NotNull T object) throws IOException {
    if (indexManager.contains(object.getKey())) {
      throw new IOException("Key already exists");
    }
    IndexRecord item = dataStorage.add(object);
    indexManager.add(object.getKey(), item);
    return item;
  }

  public boolean isFull() {
    return dataStorage.isFull();
  }

  public boolean remove(long key) {
    return indexManager.delete(key);
  }

  public @Nullable IndexGet<T> get(long key) throws IOException {
    T obj = null;
    IndexRecord item = null;
    if (key >= 0) {
      item = indexManager.get(key);
      if (item != null) {
        obj = dataStorage.get(item);
      }
    }
    if (item != null) {
      return new IndexGet<>(item, obj);
    }
    return null;
  }

  public long length() throws IOException {
    return mapChannel.size() + dataStorage.length();
  }

  public long emptySpace() {
    return indexManager.emptySpace();
  }

  public long size() {
    return indexManager.size();
  }

  public boolean isEmpty() {
    return indexManager.size() == 0;
  }

  public @NotNull Collection<Long> keepOnly(@NotNull Collection<Long> listToKeep) {
    List<Long> itemsToRemove = indexManager.keySet();
    itemsToRemove.removeIf(listToKeep::contains);
    if (!itemsToRemove.isEmpty()) {
      for (long key : itemsToRemove) {
        remove(key);
      }
    }

    if (itemsToRemove.size() != listToKeep.size()) {
      List<Long> actual = indexManager.keySet();
      listToKeep.removeIf(actual::contains);
      return listToKeep;
    }
    return new ArrayList<>();
  }

  public int removeAll(@NotNull Collection<Long> listToRemove) {
    int count =0;
    if (!listToRemove.isEmpty()) {
      for (long key : listToRemove) {
        if(remove(key)){
          count++;
        }
      }
    }
    return count;
  }


  private FileChannel openChannel(File file) throws IOException {
    StandardOpenOption[] writeOptions;
    if (sync) {
      writeOptions = new StandardOpenOption[]{CREATE, READ, WRITE, SPARSE, DSYNC};
    } else {
      writeOptions = new StandardOpenOption[]{CREATE, READ, WRITE, SPARSE};
    }
    return (FileChannel) Files.newByteChannel(file.toPath(), writeOptions);
  }

  public boolean contains(long key) {
    return indexManager.contains(key);
  }
}
