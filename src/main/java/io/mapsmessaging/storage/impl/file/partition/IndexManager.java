/*
 *
 * Copyright [2020 - 2021]   [Matthew Buckton]
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 *
 */

package io.mapsmessaging.storage.impl.file.partition;

import io.mapsmessaging.utilities.collections.MappedBufferHelper;
import io.mapsmessaging.utilities.collections.NaturalOrderedLongList;
import io.mapsmessaging.utilities.collections.bitset.ByteBufferBitSetFactoryImpl;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

public class IndexManager implements Closeable {

  private volatile boolean closed;
  private final long position;
  private final long localEnd;
  private long end;

  private final LongAdder counter;
  private final LongAdder emptySpace;

  private volatile IndexManager next;

  private @Getter final long start;
  private @Getter final int itemSize;

  private MappedByteBuffer index;

  public IndexManager(FileChannel channel) throws IOException {
    ByteBuffer header = ByteBuffer.allocate(24);

    channel.read(header);
    header.flip();
    long nextPos = header.getLong();
    start = header.getLong();
    itemSize = (int)header.getLong();
    end = start+itemSize;
    localEnd = end;
    position = channel.position();
    int totalSize = itemSize * IndexRecord.HEADER_SIZE;
    index = channel.map(MapMode.READ_WRITE, position, totalSize);
    index.load(); // Ensure the file contents are loaded
    closed = false;
    counter = new LongAdder();
    emptySpace = new LongAdder();
    walkIndex();
    if(nextPos != 0){
      channel.position(nextPos);
      next = new IndexManager(channel);
    }
    else {
      next = null;
    }
  }

  public IndexManager(long start, int itemSize, FileChannel channel) throws IOException {
    this.start = start;
    this.itemSize = itemSize;
    end = start+itemSize;
    localEnd = end;
    counter = new LongAdder();
    emptySpace = new LongAdder();

    ByteBuffer header = ByteBuffer.allocate(24);
    header.putLong(0L);
    header.putLong(start);
    header.putLong(itemSize);
    header.flip();
    channel.write(header);
    header.flip();
    position = channel.position();
    int totalSize = itemSize * IndexRecord.HEADER_SIZE;
    index = channel.map(MapMode.READ_WRITE, position, totalSize);
    index.load(); // Ensure the file contents are loaded
    IndexRecord empty = new IndexRecord();
    for(int x=0;x<itemSize;x++){
      empty.update(index); // fill with 0's
    }
    index.force(); // ensure the disk / memory are in sync
    closed = false;
    next = null;
  }

  @Override
  public void close() throws IOException {
    if(!closed) {
      closed = true;
      index.force();
      MappedBufferHelper.closeDirectBuffer(index);
      index = null; // ensure NPE rather than a full-blown JVM crash!!!
    }
    if(next != null){
      next.close(); // chain the closes
    }
  }

  public long getEnd(){
    if(next != null){
      return next.getEnd();
    }
    return end;
  }

  public int size(){
    int size = (int) counter.sum();
    if(next != null){
      size += next.size();
    }
    return size;
  }

  public int emptySpace(){
    int empty = (int) emptySpace.sum();
    if(next != null){
      empty += next.emptySpace();
    }
    return empty;
  }


  public boolean add(long key, @NotNull IndexRecord item){
    if(key>= start && key <= localEnd){
      if(key<=end){
        setMapPosition(key);
        item.update(index);
        counter.increment();
        return true;
      }
      else{
        return next.add(key, item);
      }
    }
    return false;
  }

  public @Nullable IndexRecord get(long key){
    IndexRecord item = null;
    if(key>= start && key <= localEnd){
      if(key<=end){
        setMapPosition(key);
        item = new IndexRecord(index);
        item.setKey(key);
      }
      else{
        return next.get(key);
      }
    }
    return item;
  }

  public boolean contains(long key){
    if(key>= start && key <= localEnd){
      if(key<=end){
        setMapPosition(key);
        IndexRecord item = new IndexRecord(index);
        return item.getPosition() != 0;
      }
      else{
        return next.contains(key);
      }
    }
    return false;
  }

  public boolean delete(long key){
    if(key>= start && key <= localEnd){
      if(key<=end){
        setMapPosition(key);
        IndexRecord item = new IndexRecord(index);
        if(item.getPosition() != 0) {
          counter.decrement();
          emptySpace.add(item.getLength());
          setMapPosition(key);
          IndexRecord.clear(index);
          return true;
        }
      }
      else{
        return next.delete(key);
      }
    }
    return false;
  }

  void setMapPosition(long key){
    int adjusted = (int)(key - start);
    int pos = adjusted * IndexRecord.HEADER_SIZE;
    index.position(pos);
  }

  void walkIndex(){
    IndexRecord indexRecord;
    index.position(0);
    for(int x=0;x<itemSize;x++){
      indexRecord = new IndexRecord(index);
      if(indexRecord.getPosition() != 0){
        counter.increment();
      }
      else if(indexRecord.getLength() > 0){
        emptySpace.add(indexRecord.getLength());
      }
    }
  }

  public List<Long> keySet(){
    List<Long> keys = new NaturalOrderedLongList(0, new ByteBufferBitSetFactoryImpl(8192));
    getIterator().forEachRemaining(indexRecord -> {
      if(indexRecord != null) {
        keys.add(indexRecord.getKey());
      }
    });
    return keys;
  }

  public Iterator<IndexRecord> getIterator(){
    return new HeaderIterator();
  }

  void expandHeader(long start, FileChannel channel) throws IOException {
    if(next == null) {
      long pos = channel.position();
      next = new IndexManager(start, itemSize, channel);
      channel.position(pos);
      ByteBuffer header = ByteBuffer.allocate(8);
      header.putLong(pos);
      channel.write(header); // Map in the new pointer
    }
    else{
      next.expandHeader(start, channel); // keep moving forward until you get to the end
    }
    end = start + itemSize;
  }

  public class HeaderIterator implements Iterator<IndexRecord>{

    private long key;

    public HeaderIterator(){
      key = start -1;
    }

    @Override
    public boolean hasNext() {
      return key != end;
    }

    @Override
    public IndexRecord next() {
      key++;
      while(hasNext()){
        IndexRecord item = get(key);
        if(item != null) {
          if (item.getPosition() != 0) {
            return item;
          }
        }
        key++;
      }
      return null;
    }

    @Override
    public void remove() {
      delete(key);
    }

    @Override
    public void forEachRemaining(Consumer<? super IndexRecord> action) {
      Iterator.super.forEachRemaining(action);
    }
  }

}
