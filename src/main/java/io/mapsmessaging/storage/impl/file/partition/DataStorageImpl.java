/*
 *
 *  Copyright [ 2020 - 2024 ] Matthew Buckton
 *  Copyright [ 2024 - 2026 ] MapsMessaging B.V.
 *
 *  Licensed under the Apache License, Version 2.0 with the Commons Clause
 *  (the "License"); you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at:
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *      https://commonsclause.com/
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.mapsmessaging.storage.impl.file.partition;

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.StorableFactory;
import io.mapsmessaging.storage.impl.file.FileHelper;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

import static java.nio.file.StandardOpenOption.*;

public class DataStorageImpl<T extends Storable> implements DataStorage<T> {

  private static final int HEADER_SIZE = 24;
  private static final int RECORD_HEADER_SIZE = 8;
  private static final int MAX_BUFFER_COUNT = 1024;

  private static final double VERSION = 1.0;
  public static final long UNIQUE_ID = 0xf00d0000d00f0000L;
  public static final long OPEN_STATE = 0xEFFFFFFFFFFFFFFFL;
  public static final long CLOSE_STATE = 0x0000000000000000L;

  private final long maxPartitionSize;
  private final StorableFactory<T> objectStorableFactory;
  private final String fileName;
  private final FileChannel readChannel;
  private final FileChannel writeChannel;
  private final ByteBuffer lengthBuffer;

  private volatile boolean closed;

  @Getter
  private boolean validationRequired;

  @Getter
  private boolean full;

  public DataStorageImpl(String fileName, StorableFactory<T> storableFactory, boolean sync, long maxPartitionSize)
      throws IOException {
    objectStorableFactory = storableFactory;
    this.fileName = fileName;
    this.maxPartitionSize = maxPartitionSize;
    lengthBuffer = ByteBuffer.allocate(RECORD_HEADER_SIZE);

    File file = new File(fileName);
    long length = 0;
    if (file.exists()) {
      length = file.length();
    }
    full = length > maxPartitionSize;

    StandardOpenOption[] writeOptions;
    StandardOpenOption[] readOptions;
    if (sync) {
      writeOptions = new StandardOpenOption[] {CREATE, WRITE, SPARSE, DSYNC};
      readOptions = new StandardOpenOption[] {READ, WRITE, SPARSE, DSYNC};
    } else {
      writeOptions = new StandardOpenOption[] {CREATE, WRITE, SPARSE};
      readOptions = new StandardOpenOption[] {READ, WRITE, SPARSE};
    }

    validationRequired = false;
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
      ByteBuffer header = ByteBuffer.allocate(Long.BYTES);
      writeChannel.position(0);
      header.putLong(CLOSE_STATE);
      header.flip();
      writeChannel.write(header);

      writeChannel.force(true);
      readChannel.force(true);
      writeChannel.close();
      readChannel.close();
    }
  }

  private void initialise() throws IOException {
    ByteBuffer headerValidation = ByteBuffer.allocate(HEADER_SIZE);
    headerValidation.putLong(OPEN_STATE);
    headerValidation.putLong(UNIQUE_ID);
    headerValidation.putLong(Double.doubleToLongBits(VERSION));
    headerValidation.flip();
    readChannel.write(headerValidation);
    readChannel.force(false);
  }

  private void reload() throws IOException {
    ByteBuffer headerValidation = ByteBuffer.allocate(HEADER_SIZE);
    if (readChannel.read(headerValidation) != HEADER_SIZE) {
      throw new IOException("Unable to read data storage header");
    }
    headerValidation.flip();

    validationRequired = headerValidation.getLong() != CLOSE_STATE;
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

  @Override
  public String getName() {
    return fileName;
  }

  @Override
  public void delete() throws IOException {
    close();
    FileHelper.delete(fileName);
  }

  @Override
  public IndexRecord add(@NotNull T object) throws IOException {
    long eof = writeChannel.size();
    writeChannel.position(eof);

    ByteBuffer[] buffers = objectStorableFactory.pack(object);
    ByteBuffer meta = ByteBuffer.allocate((buffers.length + 2) * Integer.BYTES);
    int len = Integer.BYTES;
    meta.position(Integer.BYTES);
    meta.putInt(buffers.length);
    for (ByteBuffer buffer : buffers) {
      int bufferLength = buffer.limit();
      len += bufferLength;
      meta.putInt(bufferLength);
    }
    meta.putInt(0, len);
    meta.flip();

    ByteBuffer[] inclusive = new ByteBuffer[buffers.length + 1];
    System.arraycopy(buffers, 0, inclusive, 1, buffers.length);
    inclusive[0] = meta;
    writeChannel.write(inclusive);

    long fileLength = writeChannel.size();
    long length = fileLength - eof;
    full = fileLength > maxPartitionSize;
    return new IndexRecord(object.getKey(), 0, eof, object.getExpiry(), (int) length);
  }

  @Override
  public @Nullable T get(IndexRecord item) throws IOException {
    if (item == null || item.getPosition() <= 0) {
      return null;
    }
    return reloadMessage(item.getPosition());
  }

  @Override
  public boolean isValid(IndexRecord item) throws IOException {
    if (item == null || item.getPosition() <= 0 || item.getLength() <= 0) {
      return false;
    }

    long fileSize = readChannel.size();
    long filePosition = item.getPosition();
    long recordEnd = filePosition + item.getLength();

    if (filePosition < HEADER_SIZE || recordEnd > fileSize || recordEnd < filePosition) {
      return false;
    }

    ByteBuffer header = ByteBuffer.allocate(RECORD_HEADER_SIZE);
    if (!readFully(header, filePosition)) {
      return false;
    }
    header.flip();

    int storedLength = header.getInt();
    int bufferCount = header.getInt();

    if (storedLength <= 0 || bufferCount <= 0 || bufferCount > MAX_BUFFER_COUNT) {
      return false;
    }

    long bufferInfoLength = (long) bufferCount * Integer.BYTES;
    long bufferInfoPosition = filePosition + RECORD_HEADER_SIZE;
    long payloadPosition = bufferInfoPosition + bufferInfoLength;

    if (payloadPosition > recordEnd || payloadPosition < bufferInfoPosition) {
      return false;
    }

    ByteBuffer bufferInfo = ByteBuffer.allocate((int) bufferInfoLength);
    if (!readFully(bufferInfo, bufferInfoPosition)) {
      return false;
    }
    bufferInfo.flip();

    long payloadLength = 0;
    for (int index = 0; index < bufferCount; index++) {
      int bufferLength = bufferInfo.getInt();
      if (bufferLength < 0) {
        return false;
      }
      payloadLength += bufferLength;
      if (payloadLength > item.getLength()) {
        return false;
      }
    }

    long payloadEnd = payloadPosition + payloadLength;
    if (payloadEnd > recordEnd || payloadEnd < payloadPosition) {
      return false;
    }

    try {
      return reloadMessage(filePosition) != null;
    } catch (IOException | RuntimeException e) {
      return false;
    }
  }

  private T reloadMessage(long filePosition) throws IOException {
    readChannel.position(filePosition);
    lengthBuffer.clear();
    if (readChannel.read(lengthBuffer) != RECORD_HEADER_SIZE) {
      throw new IOException("Unable to read data record header");
    }

    int len = lengthBuffer.getInt(0);
    T obj = null;
    if (len > 0) {
      int bufferCount = lengthBuffer.getInt(Integer.BYTES);
      ByteBuffer bufferInfo = ByteBuffer.allocate(bufferCount * Integer.BYTES);
      if (readChannel.read(bufferInfo) != bufferInfo.capacity()) {
        throw new IOException("Unable to read data record buffer metadata");
      }
      bufferInfo.flip();

      ByteBuffer[] data = new ByteBuffer[bufferCount];
      for (int index = 0; index < bufferCount; index++) {
        data[index] = ByteBuffer.allocate(bufferInfo.getInt());
      }

      readChannel.read(data);
      for (ByteBuffer buffer : data) {
        if (buffer.hasRemaining()) {
          throw new IOException("Unable to read complete data record payload");
        }
        buffer.flip();
      }
      obj = objectStorableFactory.unpack(data);
    }
    return obj;
  }

  private boolean readFully(ByteBuffer buffer, long position) throws IOException {
    long currentPosition = position;
    while (buffer.hasRemaining()) {
      int read = readChannel.read(buffer, currentPosition);
      if (read <= 0) {
        return false;
      }
      currentPosition += read;
    }
    return true;
  }

  @Override
  public long length() throws IOException {
    return readChannel.size();
  }
}