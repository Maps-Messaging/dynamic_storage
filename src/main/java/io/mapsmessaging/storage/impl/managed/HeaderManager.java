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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class HeaderManager implements Closeable {

  private volatile boolean closed;
  private final long position;
  private final long localEnd;
  private long end;

  private volatile HeaderManager prev;
  private volatile HeaderManager next;

  private @Getter final long start;
  private @Getter final int itemSize;

  private MappedByteBuffer index;

  public HeaderManager(FileChannel channel) throws IOException {
    ByteBuffer header = ByteBuffer.allocate(20);
    channel.read(header);
    header.flip();
    long nextPos = header.getLong();
    start = header.getLong();
    itemSize = header.getInt();
    end = start+itemSize;
    localEnd = end;
    position = channel.position();
    int totalSize = itemSize * HeaderItem.HEADER_SIZE;
    index = channel.map(MapMode.READ_WRITE, position, totalSize);
    closed = false;
    if(nextPos != 0){
      channel.position(nextPos);
      next = new HeaderManager(channel);
      next.prev = this;
    }
    else {
      next = null;
      prev = null;
    }
  }

  public HeaderManager(long start, int itemSize, FileChannel channel) throws IOException {
    this.start = start;
    this.itemSize = itemSize;
    end = start+itemSize;
    localEnd = end;

    ByteBuffer header = ByteBuffer.allocate(HeaderItem.HEADER_SIZE);
    header.putLong(0L);
    header.putLong(start);
    header.putInt(itemSize);
    header.flip();
    channel.write(header);
    header.flip();
    position = channel.position();
    int totalSize = itemSize * HeaderItem.HEADER_SIZE;
    index = channel.map(MapMode.READ_WRITE, position, totalSize);
    HeaderItem empty = new HeaderItem(0,0,0);
    for(int x=0;x<itemSize;x++){
      empty.update(index); // fill with 0's
    }
    index.force(); // ensure the disk / memory are in sync
    closed = false;
    next = null;
    prev = null;
  }

  @Override
  public void close() throws IOException {
    if(!closed) {
      closed = true;
      MappedBufferHelper.closeDirectBuffer(index);
      index = null; // ensure NPE rather than a full-blown JVM crash!!!
    }
    if(next != null){
      next.close(); // chain the closes
    }
  }

  void expandHeader(long start, FileChannel channel) throws IOException {
    if(next == null) {
      long pos = channel.position();
      next = new HeaderManager(start, itemSize, channel);
      next.prev = this;
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

  public boolean add(long key, @NotNull HeaderItem item){
    if(key>= start && key <= localEnd){
      if(key<=end){
        setMapPosition(key);
        item.update(index);
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
      }
      else{
        return next.get(key);
      }
    }
    return item;
  }

  public boolean delete(long key){
    if(key>= start && key <= localEnd){
      if(key<=end){
        setMapPosition(key);
        HeaderItem.clear(index);
      }
      else{
        return next.delete(key);
      }
    }
    return false;
  }

  void setMapPosition(long key){
    int adjusted = (int)(key - start);
    long pos = position + (long)adjusted * HeaderItem.HEADER_SIZE;
    index.position((int)pos);
  }
}
