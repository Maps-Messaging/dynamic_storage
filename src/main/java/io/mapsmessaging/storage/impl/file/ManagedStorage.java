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

package io.mapsmessaging.storage.impl.file;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.DSYNC;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.SPARSE;
import static java.nio.file.StandardOpenOption.WRITE;

import io.mapsmessaging.storage.Factory;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class ManagedStorage <T extends Storable> implements Storage<T> {

  private static final double VERSION = 1.0;
  private static final long UNIQUE_ID = 0xf00d0000d00f0000L;
  private static final long OPEN_STATE = 0xEFFFFFFFFFFFFFFFL;
  private static final long CLOSE_STATE = 0x0000000000000000L;

  private static final int ITEM_COUNT = 8192;

  private HeaderManager headerManager;
  private final Factory<T> objectFactory;
  private final String fileName;
  private final FileChannel readChannel;
  private final FileChannel writeChannel;
  private final ByteBuffer lengthBuffer;

  private volatile boolean closed;

  public ManagedStorage(String fileName, Factory<T> factory, boolean sync) throws IOException {
    objectFactory = factory;
    this.fileName = fileName;
    lengthBuffer = ByteBuffer.allocate(8);
    File file = new File(fileName);
    long length = 0;
    if (file.exists()) {
      length = file.length();
    }
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
    if(length != 0){
      reload(length);
    }
    else{
      initialise();
    }
    closed = false;
  }

  @Override
  public void close() throws IOException {
    if(!closed) {
      closed = true;
      ByteBuffer header = ByteBuffer.allocate(8);
      header.putLong(CLOSE_STATE);
      writeChannel.write(header);

      headerManager.close();

      writeChannel.force(true);
      readChannel.force(true);
      writeChannel.close();
      readChannel.close();
    }
  }

  private void initialise() throws IOException {
    ByteBuffer headerValidation = ByteBuffer.allocate(32);
    headerValidation.putLong(OPEN_STATE);
    headerValidation.putLong(UNIQUE_ID);
    headerValidation.putLong(Double.doubleToLongBits(VERSION));
    headerValidation.putLong(ITEM_COUNT);
    headerValidation.flip();
    readChannel.write(headerValidation);
    headerManager= new HeaderManager(0L, ITEM_COUNT, readChannel);
    readChannel.force(false);
  }

  private void reload(long length)throws IOException{
    ByteBuffer headerValidation = ByteBuffer.allocate(32);
    readChannel.read(headerValidation);
    headerValidation.flip();
    boolean wasClosed = headerValidation.getLong() != CLOSE_STATE;
    if(headerValidation.getLong() != UNIQUE_ID){
      throw new IOException("Unexpected file identifier located");
    }
    if(Double.longBitsToDouble(headerValidation.getLong()) != VERSION){
      throw new IOException("Unexpected file version");
    }
    if(headerValidation.getLong() != ITEM_COUNT){
      throw new IOException("Unexpected item count");
    }
    headerManager = new HeaderManager(readChannel);

    headerValidation.flip();
    headerValidation.putLong(0,OPEN_STATE);
    readChannel.position(0);
    readChannel.write(headerValidation);
    readChannel.force(false);
  }

  @Override
  public String getName() {
    return fileName;
  }

  @Override
  public void delete() throws IOException {
    close();
    File path = new File(fileName);
    Files.delete(path.toPath());
  }

  @Override
  public void add(@NotNull T object) throws IOException {
    long eof = writeChannel.size();
    writeChannel.position(eof);
    ByteBuffer[] buffers = object.write();
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
    long length = writeChannel.size() - eof;
    HeaderItem item = new HeaderItem(0, eof, 0, length);
    headerManager.add(object.getKey(), item);
  }

  @Override
  public boolean remove(long key) throws IOException {
    return headerManager.delete(key);
  }

  @Override
  public @Nullable T get(long key) throws IOException {
    T obj = null;
    if (key >= 0) {
      HeaderItem item = headerManager.get(key);
      if(item != null){
        long pos = item.getPosition();
        obj = reloadMessage(pos);
      }
    }
    return obj;
  }

  @Override
  public long size() throws IOException {
    return headerManager.size();
  }

  @Override
  public boolean isEmpty() {
    return headerManager.size() == 0;
  }

  @Override
  public @NotNull List<Long> keepOnly(@NotNull List<Long> listToKeep) throws IOException {
    List<Long> itemsToRemove = headerManager.keySet();
    itemsToRemove.removeIf(listToKeep::contains);
    if (!itemsToRemove.isEmpty()) {
      for (long key : itemsToRemove) {
        remove(key);
      }
    }

    if (itemsToRemove.size() != listToKeep.size()) {
      List<Long> actual = headerManager.keySet();
      listToKeep.removeIf(actual::contains);
      return listToKeep;
    }

    return new ArrayList<>();
  }

  private T reloadMessage(long filePosition) throws IOException {
    readChannel.position(filePosition);
    lengthBuffer.clear();
    readChannel.read(lengthBuffer);
    int len = lengthBuffer.getInt(0);
    T obj = null;
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
      obj = objectFactory.create();
      obj.read(data);
    }
    return obj;
  }

}
