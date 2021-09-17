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

import io.mapsmessaging.storage.Factory;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.impl.BaseIndexStorage;
import io.mapsmessaging.storage.impl.BufferObjectReader;
import io.mapsmessaging.storage.impl.BufferObjectWriter;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import org.jetbrains.annotations.NotNull;

public class SeekableChannelStorage<T extends Storable> extends BaseIndexStorage<T> {

  private final Factory<T> objectFactory;
  private final String fileName;
  private final SeekableByteChannel readChannel;
  private final BufferObjectReader reader;

  private final SeekableByteChannel writeChannel;
  private final BufferObjectWriter writer;

  private final ByteBuffer lengthBuffer;
  private final ByteBuffer writeBuffer;
  private final ByteBuffer readBuffer;

  public SeekableChannelStorage(String fileName, Factory<T> factory, boolean sync) throws IOException {
    objectFactory = factory;
    this.fileName = fileName;
    File file = new File(fileName);
    long length = 0;
    if (file.exists()) {
      length = file.length();
    }
    lengthBuffer = ByteBuffer.allocate(4);
    writeBuffer = ByteBuffer.allocateDirect(1024 * 1024);
    readBuffer = ByteBuffer.allocateDirect(1024 * 1024);

    if (sync) {
      writeChannel = Files.newByteChannel(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.DSYNC);
    } else {
      writeChannel = Files.newByteChannel(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
    }
    readChannel = Files.newByteChannel(file.toPath(), StandardOpenOption.READ);
    reader = new BufferObjectReader(readBuffer);
    writer = new BufferObjectWriter(writeBuffer);
    reload(length);
  }


  private void reload(long eof) throws IOException {
    long pos = 0;
    while (pos != eof) {
      T obj = reloadMessage(pos);
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
    writeBuffer.clear();
    writeBuffer.position(4); // Skip the first 4 bytes so we can set the full length of the buffer
    obj.write(writer);
    int len = writeBuffer.position() - 4;
    writeBuffer.putInt(0, len);
    writeBuffer.flip();
    writeChannel.write(writeBuffer);
  }

  @Override
  public boolean remove(long key) throws IOException {
    Long pos = index.remove(key);
    if (pos != null) {
      long eof = writeChannel.position();
      lengthBuffer.clear();
      readChannel.position(pos);
      readChannel.read(lengthBuffer);
      int len = lengthBuffer.getInt(0);
      len = len * -1;
      lengthBuffer.putInt(0, len);
      writeChannel.position(pos);
      lengthBuffer.flip();
      writeChannel.write(lengthBuffer);
      writeChannel.position(eof);
      lengthBuffer.clear();
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
        obj = reloadMessage(pos);
      }
    }
    return obj;
  }

  private T reloadMessage(long filePosition) throws IOException {
    readChannel.position(filePosition);
    lengthBuffer.clear();
    readChannel.read(lengthBuffer);
    int len = lengthBuffer.getInt(0);
    if (len > 0) {
      readBuffer.limit(len);
      readChannel.read(readBuffer);
      readBuffer.flip();
      T obj = objectFactory.create();
      obj.read(reader);
      readBuffer.clear();
      return obj;
    } else {
      readChannel.position(readChannel.position() + (len * -1)); // skip
      return null;
    }
  }

  @Override
  public void close() throws IOException {
    readChannel.close();
    writeChannel.close();
  }
}