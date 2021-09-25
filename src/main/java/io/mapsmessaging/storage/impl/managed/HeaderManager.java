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

package io.mapsmessaging.storage.impl.managed;

import io.mapsmessaging.utilities.collections.MappedBufferHelper;
import io.mapsmessaging.utilities.collections.NaturalOrderedLongList;
import io.mapsmessaging.utilities.collections.bitset.ByteBufferBitSetFactoryImpl;
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
import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class HeaderManager implements Closeable {

  private volatile boolean closed;
  private final long position;
  private final long localEnd;
  private long end;

  private final LongAdder counter;
  private final LongAdder emptySpace;

  private volatile HeaderManager next;

  private @Getter final long start;
  private @Getter final int itemSize;

  private MappedByteBuffer index;

  public HeaderManager(FileChannel channel) throws IOException {
    ByteBuffer header = ByteBuffer.allocate(24);

    channel.read(header);
    header.flip();
    long nextPos = header.getLong();
    start = header.getLong();
    itemSize = (int)header.getLong();
    end = start+itemSize;
    localEnd = end;
    position = channel.position();
    int totalSize = itemSize * HeaderItem.HEADER_SIZE;
    index = channel.map(MapMode.READ_WRITE, position, totalSize);
    index.load(); // Ensure the file contents are loaded
    closed = false;
    counter = new LongAdder();
    emptySpace = new LongAdder();
    walkIndex();
    if(nextPos != 0){
      channel.position(nextPos);
      next = new HeaderManager(channel);
    }
    else {
      next = null;
    }
  }

  public HeaderManager(long start, int itemSize, FileChannel channel) throws IOException {
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
    int totalSize = itemSize * HeaderItem.HEADER_SIZE;
    index = channel.map(MapMode.READ_WRITE, position, totalSize);
    index.load(); // Ensure the file contents are loaded
    HeaderItem empty = new HeaderItem();
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


  public boolean add(long key, @NotNull HeaderItem item){
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

  public @Nullable HeaderItem get(long key){
    HeaderItem item = null;
    if(key>= start && key <= localEnd){
      if(key<=end){
        setMapPosition(key);
        item = new HeaderItem(index);
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
        HeaderItem item = new HeaderItem(index);
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
        HeaderItem item = new HeaderItem(index);
        if(item.getPosition() != 0) {
          counter.decrement();
          emptySpace.add(item.getLength());
          setMapPosition(key);
          HeaderItem.clear(index);
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
    int pos = adjusted * HeaderItem.HEADER_SIZE;
    index.position(pos);
  }

  void walkIndex(){
    HeaderItem headerItem;
    index.position(0);
    for(int x=0;x<itemSize;x++){
      headerItem = new HeaderItem(index);
      if(headerItem.getPosition() != 0){
        counter.increment();
      }
      else if(headerItem.getLength() > 0){
        emptySpace.add(headerItem.getLength());
      }
    }
  }

  public List<Long> keySet(){
    Iterator<HeaderItem> itemIterator = getIterator();
    NaturalOrderedLongList keys = new NaturalOrderedLongList(0, new ByteBufferBitSetFactoryImpl(8192));
    while(itemIterator.hasNext()){
      HeaderItem item = itemIterator.next();
      if(item != null) {
        keys.add(item.getKey());
      }
    }
    return keys;
  }

  public Iterator<HeaderItem> getIterator(){
    return new HeaderIterator();
  }

  void expandHeader(long start, FileChannel channel) throws IOException {
    if(next == null) {
      long pos = channel.position();
      next = new HeaderManager(start, itemSize, channel);
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

  public class HeaderIterator implements Iterator<HeaderItem>{

    private long key;

    public HeaderIterator(){
      key = start -1;
    }

    @Override
    public boolean hasNext() {
      return key != end;
    }

    @Override
    public HeaderItem next() {
      key++;
      while(hasNext()){
        HeaderItem item = get(key);
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
    public void forEachRemaining(Consumer<? super HeaderItem> action) {
      Iterator.super.forEachRemaining(action);
    }
  }

}
