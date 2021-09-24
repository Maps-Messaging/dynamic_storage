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

package io.mapsmessaging.storage.impl.seekable;

import static java.nio.file.StandardOpenOption.*;

import io.mapsmessaging.storage.Factory;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.impl.BaseIndexStorage;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import org.jetbrains.annotations.NotNull;

public class SeekableChannelStorage<T extends Storable> extends BaseIndexStorage<T> {

  private final Factory<T> objectFactory;
  private final String fileName;
  private final FileChannel readChannel;
  private final FileChannel writeChannel;

  private final ByteBuffer lengthBuffer;

  public SeekableChannelStorage(String fileName, Factory<T> factory, boolean sync) throws IOException {
    objectFactory = factory;
    this.fileName = fileName;
    File file = new File(fileName);
    long length = 0;
    if (file.exists()) {
      length = file.length();
    }
    lengthBuffer = ByteBuffer.allocate(8);

    StandardOpenOption[] writeOptions;
    StandardOpenOption[] readOptions;
    if (sync) {
      writeOptions = new StandardOpenOption[]{CREATE, WRITE, SPARSE, DSYNC};
      readOptions = new StandardOpenOption[]{READ, WRITE, SPARSE, DSYNC};
    }
    else{
      writeOptions = new StandardOpenOption[]{CREATE, WRITE, SPARSE};
      readOptions = new StandardOpenOption[]{READ, WRITE, SPARSE};
    }

    writeChannel = (FileChannel) Files.newByteChannel(file.toPath(), writeOptions);
    readChannel = (FileChannel) Files.newByteChannel(file.toPath(), readOptions);
    reload(length);
  }


  private void reload(long eof) throws IOException {
    long pos = 0;
    while (pos != eof) {
      T obj = reloadMessage(pos, true);
      if (obj != null) {
        index.put(obj.getKey(), pos);
      }
      pos = readChannel.position();
    }
  }

  @Override
  public String getName() {
    return fileName;
  }

  @Override
  public void delete() throws IOException {
    readChannel.close();
    writeChannel.close();
    File path = new File(fileName);
    Files.delete(path.toPath());
  }

  @Override
  public void add(@NotNull T obj) throws IOException {
    index.put(obj.getKey(), writeChannel.position());
    ByteBuffer[] buffers = obj.write();
    ByteBuffer meta = ByteBuffer.allocate((buffers.length+2)*4);
    int len = 4; // Initial address
    meta.position(4);
    meta.putInt(buffers.length);
    for(ByteBuffer buffer:buffers){
      int bufLen = buffer.limit();
      len +=bufLen;
      meta.putInt(bufLen);
    }
    meta.putInt(0, len);
    meta.flip();
    ByteBuffer[] inclusive = new ByteBuffer[buffers.length+1];
    System.arraycopy(buffers, 0, inclusive, 1, buffers.length);
    inclusive[0] = meta;
    writeChannel.write(inclusive);
  }

  @Override
  public boolean remove(long key) throws IOException {
    Long pos = index.remove(key);
    if (pos != null) {
      lengthBuffer.clear();
      readChannel.position(pos);
      readChannel.read(lengthBuffer);
      int len = lengthBuffer.getInt(0);
      len = len * -1;
      lengthBuffer.putInt(0, len);
      lengthBuffer.putInt(4, 0);
      readChannel.position(pos);
      lengthBuffer.flip();
      readChannel.write(lengthBuffer);
      return true;
    }
    return false;
  }

  @Override
  public T get(long key) throws IOException {
    T obj = null;
    if (key >= 0) {
      Long pos = index.get(key);
      if (pos != null) {
        obj = reloadMessage(pos, false);
      }
    }
    return obj;
  }

  private T reloadMessage(long filePosition, boolean skip) throws IOException {
    readChannel.position(filePosition);
    lengthBuffer.clear();
    readChannel.read(lengthBuffer);
    int len = lengthBuffer.getInt(0);
    if (len > 0) {
      int bufferCount = lengthBuffer.getInt(4);
      ByteBuffer bufferInfo = ByteBuffer.allocate((bufferCount)*4);
      readChannel.read(bufferInfo);
      bufferInfo.flip();
      ByteBuffer[] data = new ByteBuffer[bufferCount];
      for(int x=0;x<bufferCount;x++){
        data[x] = ByteBuffer.allocate(bufferInfo.getInt());
      }
      readChannel.read(data);
      for(ByteBuffer buffer:data){
        buffer.flip();
      }
      T obj = objectFactory.create();
      obj.read(data);
      return obj;
    } else {
      if(skip) {
        readChannel.position(readChannel.position() + (len * -1)); // skip
      }
      return null;
    }
  }

  @Override
  public void close() throws IOException {
    readChannel.close();
    writeChannel.close();
  }
}