package io.mapsmessaging.storage.impl;

import io.mapsmessaging.storage.Factory;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.storage.streams.BufferObjectReader;
import io.mapsmessaging.storage.streams.BufferObjectWriter;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jetbrains.annotations.NotNull;

public class SeekableChannelStorage<T extends Storable> implements Storage<T> {

  private final Factory<T> objectFactory;
  private final Map<Long, Long> index;
  private final SeekableByteChannel readChannel;
  private final BufferObjectReader reader;

  private final SeekableByteChannel writeChannel;
  private final BufferObjectWriter writer;

  private final ByteBuffer lengthBuffer;
  private final ByteBuffer writeBuffer;
  private final ByteBuffer readBuffer;

  public SeekableChannelStorage(String name, Factory<T> factory) throws IOException {
    objectFactory = factory;
    File file = new File(name);
    long length = 0;
    if (file.exists()) {
      length = file.length();
    }
    lengthBuffer = ByteBuffer.allocateDirect(1024 * 1024);
    writeBuffer = ByteBuffer.allocateDirect(1024 * 1024);
    readBuffer = ByteBuffer.allocateDirect(1024 * 1024);

    writeChannel = Files.newByteChannel(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
    readChannel = Files.newByteChannel(file.toPath(), StandardOpenOption.READ);
    reader = new BufferObjectReader(readBuffer);
    writer = new BufferObjectWriter(writeBuffer);
    index = new LinkedHashMap<>();
    reload(length);
  }

  @Override
  public boolean isEmpty() {
    return false;
  }

  @Override
  public @NotNull List<Long> keepOnly(@NotNull List<Long> listToKeep) throws IOException {
    Set<Long> itemsToRemove = index.keySet();
    itemsToRemove.removeIf(listToKeep::contains);
    if(!itemsToRemove.isEmpty()){
      for(long key:itemsToRemove){
        remove(key);
      }
    }

    if(itemsToRemove.size() != listToKeep.size()){
      Set<Long> actual = index.keySet();
      listToKeep.removeIf(actual::contains);
      return listToKeep;
    }
    return new ArrayList<>();
  }

  private void reload(long eof) throws IOException {
    long pos = 0;
    while (pos != eof) {
      T obj = reloadMessage(pos);
      pos = readChannel.position();
      if (obj != null) {
        index.put(obj.getKey(),pos);
      }
    }
  }

  @Override
  public void delete() throws IOException {
    readChannel.close();
    writeChannel.close();
  }

  @Override
  public void add(T obj) throws IOException {
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
  public void remove(long key) throws IOException {
    Long pos = index.remove(key);
    if (pos != null) {
      long eof = writeChannel.position();
      lengthBuffer.clear();
      readChannel.position(pos);
      readChannel.read(lengthBuffer);
      int len = lengthBuffer.getInt(0);
      len = len * -1;
      lengthBuffer.putInt(0, len);
      writeChannel.position(pos - 4);
      lengthBuffer.flip();
      writeChannel.write(lengthBuffer);
      writeChannel.position(eof);
      lengthBuffer.clear();
    }
  }

  @Override
  public T get(long key) throws IOException {
    T obj = null;
    if (key >= 0) {
      Long pos = index.get(key);
      if(pos != null) {
        obj = reloadMessage(pos);
      }
    }
    return obj;
  }

  @Override
  public long size() throws IOException {
    return index.size();
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