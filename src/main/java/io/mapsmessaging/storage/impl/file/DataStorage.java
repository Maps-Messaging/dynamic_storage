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
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class DataStorage<N extends Storable> implements Closeable {

  private static final double VERSION = 1.0;
  private static final long UNIQUE_ID = 0xf00d0000d00f0000L;
  private static final long OPEN_STATE = 0xEFFFFFFFFFFFFFFFL;
  private static final long CLOSE_STATE = 0x0000000000000000L;

  private final Factory<N> objectFactory;
  private final String fileName;
  private final FileChannel readChannel;
  private final FileChannel writeChannel;
  private final ByteBuffer lengthBuffer;

  private volatile boolean closed;

  public DataStorage(String fileName, Factory<N> factory, boolean sync) throws IOException {
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
    } else {
      writeOptions = new StandardOpenOption[]{CREATE, WRITE, SPARSE};
      readOptions = new StandardOpenOption[]{READ, WRITE, SPARSE};
    }

    writeChannel = (FileChannel) Files.newByteChannel(file.toPath(), writeOptions);
    readChannel = (FileChannel) Files.newByteChannel(file.toPath(), readOptions);
    if (length != 0) {
      reload();
    } else {
      initialise();
    }
    closed = false;
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      closed = true;
      ByteBuffer header = ByteBuffer.allocate(8);
      header.putLong(CLOSE_STATE);
      writeChannel.write(header);

      writeChannel.force(true);
      readChannel.force(true);
      writeChannel.close();
      readChannel.close();
    }
  }

  private void initialise() throws IOException {
    ByteBuffer headerValidation = ByteBuffer.allocate(24);
    headerValidation.putLong(OPEN_STATE);
    headerValidation.putLong(UNIQUE_ID);
    headerValidation.putLong(Double.doubleToLongBits(VERSION));
    headerValidation.flip();
    readChannel.write(headerValidation);
    readChannel.force(false);
  }

  private void reload() throws IOException {
    ByteBuffer headerValidation = ByteBuffer.allocate(24);
    readChannel.read(headerValidation);
    headerValidation.flip();
    boolean wasClosed = headerValidation.getLong() != CLOSE_STATE;
    if (headerValidation.getLong() != UNIQUE_ID) {
      throw new IOException("Unexpected file identifier located");
    }
    if (Double.longBitsToDouble(headerValidation.getLong()) != VERSION) {
      throw new IOException("Unexpected file version");
    }

    headerValidation.flip();
    headerValidation.putLong(0, OPEN_STATE);
    readChannel.position(0);
    readChannel.write(headerValidation);
    readChannel.force(false);
  }

  public String getName() {
    return fileName;
  }

  public void delete() throws IOException {
    close();
    File path = new File(fileName);
    Files.delete(path.toPath());
  }

  public HeaderItem add(@NotNull N object) throws IOException {
    long eof = writeChannel.size();
    writeChannel.position(eof);
    ByteBuffer[] buffers = object.write();
    ByteBuffer meta = ByteBuffer.allocate((buffers.length + 2) * 4);
    int len = 4; // Initial address
    meta.position(4);
    meta.putInt(buffers.length);
    for (ByteBuffer buffer : buffers) {
      int bufLen = buffer.limit();
      len += bufLen;
      meta.putInt(bufLen);
    }
    meta.putInt(0, len);
    meta.flip();
    ByteBuffer[] inclusive = new ByteBuffer[buffers.length + 1];
    System.arraycopy(buffers, 0, inclusive, 1, buffers.length);
    inclusive[0] = meta;
    writeChannel.write(inclusive);
    long length = writeChannel.size() - eof;
    HeaderItem item = new HeaderItem(0, eof, 0, length);
    item.setKey(object.getKey());
    return item;
  }

  public @Nullable N get(HeaderItem item) throws IOException {
    N obj = null;
    if (item != null) {
      long pos = item.getPosition();
      obj = reloadMessage(pos);
    }
    return obj;
  }

  private N reloadMessage(long filePosition) throws IOException {
    readChannel.position(filePosition);
    lengthBuffer.clear();
    readChannel.read(lengthBuffer);
    int len = lengthBuffer.getInt(0);
    N obj = null;
    if (len > 0) {
      int bufferCount = lengthBuffer.getInt(4);
      ByteBuffer bufferInfo = ByteBuffer.allocate((bufferCount) * 4);
      readChannel.read(bufferInfo);
      bufferInfo.flip();
      ByteBuffer[] data = new ByteBuffer[bufferCount];
      for (int x = 0; x < bufferCount; x++) {
        data[x] = ByteBuffer.allocate(bufferInfo.getInt());
      }
      readChannel.read(data);
      for (ByteBuffer buffer : data) {
        buffer.flip();
      }
      obj = objectFactory.create();
      obj.read(data);
    }
    return obj;
  }
}