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

import io.mapsmessaging.storage.impl.file.tasks.MemoryMapLoadTask;
import io.mapsmessaging.utilities.collections.MappedBufferHelper;
import io.mapsmessaging.utilities.collections.NaturalOrderedLongList;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class IndexManager implements Closeable {

  private static final int HEADER_SIZE = 16;

  @Getter
  private final long start;

  @Getter
  private long end;

  @Getter
  private final List<Long> expiryIndex;

  private final FileChannel channel;
  private final long position;
  private final long localEnd;

  private final LongAdder counter;
  private final LongAdder emptySpace;
  private MappedByteBuffer index;

  private volatile long maxKey;
  private volatile boolean closed;
  private volatile boolean paused;

  private final AtomicBoolean loaded;


  public IndexManager(FileChannel channel) throws IOException {
    loaded = new AtomicBoolean(false);
    this.channel = channel;
    position = channel.position();
    ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
    channel.read(header);
    header.flip();
    start = header.getLong();
    end = header.getLong();
    localEnd = end;

    int totalSize = (int) ((end - start) + 1) * IndexRecord.HEADER_SIZE;
    closed = false;
    counter = new LongAdder();
    emptySpace = new LongAdder();
    expiryIndex = new NaturalOrderedLongList();
    maxKey = 0;
    paused = false;
    index = channel.map(MapMode.READ_WRITE, position + HEADER_SIZE, totalSize);
  }

  public IndexManager(long start, int itemSize, FileChannel channel) throws IOException {
    loaded = new AtomicBoolean(false);
    this.channel = channel;
    position = channel.position();
    this.start = start;
    end = (start + itemSize)-1;

    localEnd = end;
    counter = new LongAdder();
    emptySpace = new LongAdder();

    ByteBuffer header = ByteBuffer.allocate(16);
    header.putLong(start);
    header.putLong(end);
    header.flip();
    channel.write(header);
    header.flip();
    int totalSize = itemSize * IndexRecord.HEADER_SIZE;
    // This block basically moves to the end of the file -1
    // and writes 1 byte. For a sparse file it will preallocate the file and zero fill for us at no cost
    // for file systems with NO sparse support it will be zero filled and will take some time
    ByteBuffer sparseAllocate = ByteBuffer.allocate(1);
    channel.position(position + HEADER_SIZE + totalSize - 1);
    channel.write(sparseAllocate);
    channel.position(position); // Move back
    expiryIndex = new NaturalOrderedLongList();
    closed = false;
    index = channel.map(MapMode.READ_WRITE, position + HEADER_SIZE, totalSize);
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      waitForLoad();
      closed = true;
      index.force();
      MappedBufferHelper.closeDirectBuffer(index);
      index = null; // ensure NPE rather than a full-blown JVM crash!!!
    }
  }

  public void pause() {
    if (!paused) {
      waitForLoad();
      paused = true;
      index.force();
      MappedBufferHelper.closeDirectBuffer(index);
      index = null; // ensure NPE rather than a full-blown JVM crash!!!
    }
  }

  public void resume(FileChannel channel) throws IOException {
    if (paused) {
      waitForLoad();
      paused = false;
      int totalSize = (int) ((end - start) + 1) * IndexRecord.HEADER_SIZE;
      index = channel.map(MapMode.READ_WRITE, position + HEADER_SIZE, totalSize);
      index.load(); // Ensure the file contents are loaded
    }
  }

  public long getMaxKey(){
    waitForLoad();
    return maxKey;
  }

  public void scanForExpired(Queue<Long> expiredList) {
    if (!expiryIndex.isEmpty()) {
      waitForLoad();
      Iterator<Long> expiryIterator = expiryIndex.listIterator();
      long now = System.currentTimeMillis();
      while (expiryIterator.hasNext()) {
        long key = expiryIterator.next();
        IndexRecord indexRecord = get(key);
        if (indexRecord != null) {
          if (indexRecord.getExpiry() < now) {
            expiredList.add(indexRecord.getKey());
          }
        } else {
          expiryIterator.remove();
        }
      }
    }
  }

  public void setEnd(long key) throws IOException {
    waitForLoad();
    end = key;
    channel.position(position + 8);
    ByteBuffer header = ByteBuffer.allocate(8);
    header.putLong(key);
    header.flip();
    channel.write(header);
  }

  public int size() {
    waitForLoad();
    return (int) counter.sum();
  }

  public long emptySpace() {
    waitForLoad();
    return (int) emptySpace.sum();
  }


  public boolean add(long key, @NotNull IndexRecord item) {
    waitForLoad();
    if (key >= start && key <= localEnd && !closed && key <= end) {
      if (item.getExpiry() > 0) {
        expiryIndex.add(key);
      }
      setMapPosition(key);
      item.update(index);
      counter.increment();
      return true;
    }
    return false;
  }

  public @Nullable IndexRecord get(long key) {
    waitForLoad();
    IndexRecord item = null;
    if (key >= start && key <= localEnd && !closed && key <= end) {
      setMapPosition(key);
      item = new IndexRecord(index);
      item.setKey(key);
    }
    return item;
  }

  public boolean contains(long key) {
    waitForLoad();
    if (key >= start && key <= localEnd && !closed && key <= end) {
      setMapPosition(key);
      IndexRecord item = new IndexRecord(index);
      return item.getPosition() != 0;
    }
    return false;
  }

  public boolean delete(long key) {
    waitForLoad();
    return delete(key, false);
  }

  boolean delete(long key, boolean override) {
    if (key >= start && key <= localEnd && !closed && key <= end) {
      setMapPosition(key, override);
      IndexRecord item = new IndexRecord(index);
      if (item.getPosition() > 0) {
        expiryIndex.remove(key);
        counter.decrement();
        emptySpace.add(item.getLength());
        setMapPosition(key, true);
        // Mark it as deleted, so on reload we can get the total length and key
        IndexRecord indexRecord = new IndexRecord(0, -1, 0, item.getLength());
        indexRecord.update(index);
        return true;
      }
    }
    return false;
  }
  void setMapPosition(long key) {
    setMapPosition(key, false);
  }

  void setMapPosition(long key, boolean override) {
    if(!override) waitForLoad();
    int adjusted = (int) (key - start);
    int pos = adjusted * IndexRecord.HEADER_SIZE;
    index.position(pos);
  }

  List<Long> walkIndex() {
    List<Long> expired = new ArrayList<>();
    index.position(0);
    int size = (int) (end - start) + 1;
    long now = System.currentTimeMillis();
    for (int x = 0; x < size; x++) {
      validateIndexRecord(x, new IndexRecord(index), now, expired);
    }
    return expired;
  }

  private void validateIndexRecord(int index, IndexRecord indexRecord, long now, List<Long> expired) {
    long positionIndex = indexRecord.getPosition();
    if (positionIndex != 0) {
      maxKey = index;
      if (positionIndex > 0) {
        counter.increment();
        checkExpiryDetails(indexRecord, now, expired);
      }
    } else{
      long indexLength = indexRecord.getLength();
      if (indexLength > 0) {
        emptySpace.add(indexLength);
      }
    }
  }

  private void checkExpiryDetails(IndexRecord indexRecord, long now, List<Long> expired) {
    long expiry = indexRecord.getExpiry();
    if (expiry != 0) {
      if (expiry > now) {
        expiryIndex.add(indexRecord.getKey());
      } else {
        expired.add(indexRecord.getKey());
      }
    }
  }

  public List<Long> keySet() {
    waitForLoad();
    List<Long> keys = new NaturalOrderedLongList();
    getIterator().forEachRemaining(indexRecord -> {
      if (indexRecord != null && indexRecord.getLength() != 0) {
        keys.add(indexRecord.getKey());
      }
    });
    return keys;
  }

  public Iterator<IndexRecord> getIterator() {
    return new HeaderIterator();
  }

  private void waitForLoad(){
    while(!loaded.get()){
      LockSupport.parkNanos(10000);
    }
  }

  public void loadMap(boolean walkIndex){
    try {
      index.load(); // Ensure the file contents are loaded
      if(walkIndex) {
        List<Long> expired = walkIndex();
        for (Long key : expired) {
          delete(key, true);
        }
      }
    } finally {
      loaded.set(true); // pass any exception to another call
    }
  }

  public MemoryMapLoadTask queueTask(boolean walkIndex) {
    return new MemoryMapLoadTask(this, walkIndex);
  }

  public class HeaderIterator implements Iterator<IndexRecord> {

    private long key;
    private IndexRecord next;
    private IndexRecord current;

    public HeaderIterator() {
      key = start;
      current = null;
      next = locateNext();
    }

    @Override
    public boolean hasNext() {
      return next != null;
    }

    private IndexRecord locateNext() {
      IndexRecord item = null;
      while (key <= end && item == null) {
        item = get(key);
        key++;
      }
      return item;
    }

    @Override
    public IndexRecord next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      current = next;
      next = locateNext();
      return current;
    }

    @Override
    public void remove() {
      delete(current.getKey());
    }

    @Override
    public void forEachRemaining(Consumer<? super IndexRecord> action) {
      Iterator.super.forEachRemaining(action);
    }
  }

}
