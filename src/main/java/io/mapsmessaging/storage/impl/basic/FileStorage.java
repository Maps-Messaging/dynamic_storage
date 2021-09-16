package io.mapsmessaging.storage.impl.basic;

import io.mapsmessaging.storage.Factory;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.storage.impl.RandomAccessFileObjectReader;
import io.mapsmessaging.storage.impl.RandomAccessFileObjectWriter;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jetbrains.annotations.NotNull;

public class FileStorage <T extends Storable> implements Storage<T> {
  private final Map<Long, Long> index;
  private final RandomAccessFile randomAccessWriteFile;
  private final RandomAccessFile randomAccessReadFile;
  private final RandomAccessFileObjectWriter writer;
  private final RandomAccessFileObjectReader reader;
  private final String fileName;
  private final Factory<T> factory;

  public FileStorage(String fileName, Factory<T> factory) throws IOException {
    this.fileName = fileName;
    this.factory = factory;
    index = new LinkedHashMap<>();
    randomAccessWriteFile = new RandomAccessFile(fileName, "rw");
    randomAccessReadFile = new RandomAccessFile(fileName, "rw");
    writer = new RandomAccessFileObjectWriter(randomAccessWriteFile);
    reader = new RandomAccessFileObjectReader(randomAccessReadFile);
    if (randomAccessReadFile.length() != 0) {
      reload();
      randomAccessWriteFile.seek(randomAccessReadFile.getFilePointer());
    }
  }

  private void reload() throws IOException {
    long pos = 0;
    long eof = randomAccessReadFile.length();
    while (pos != eof) {
      T entry = factory.create();
      entry.read(reader);
      index.put(entry.getKey(),pos);
      pos = randomAccessReadFile.getFilePointer();
    }
  }

  @Override
  public void close() throws IOException {
    randomAccessWriteFile.close();
    randomAccessReadFile.close();
  }

  @Override
  public String getName() {
    return fileName;
  }

  @Override
  public void delete() throws IOException {
    randomAccessWriteFile.close();
    randomAccessReadFile.close();
    File file = new File(getName());
    Files.delete(file.toPath());
  }

  @Override
  public void add(T entry) throws IOException {
    long pos = randomAccessWriteFile.getFilePointer();
    randomAccessWriteFile.seek(pos);
    entry.write(writer);
    index.put(entry.getKey(),  pos);
  }

  @Override
  public T get(long key) throws IOException {
    T entry = null;
    if (key >= 0) {
      Long pos = index.get(key);
      if (pos != null) {
        randomAccessReadFile.seek(pos);
        entry = factory.create();
        entry.read(reader);
      }
    }
    return entry;
  }

  @Override
  public long size() throws IOException {
    return index.size();
  }

  @Override
  public boolean remove(long key) throws IOException {
    Long pos = index.remove(key);
    if(pos != null) {
      randomAccessReadFile.seek(pos);
      randomAccessReadFile.writeLong(-1);
    }
    return pos != null;
  }

  @Override
  public boolean isEmpty() {
    return index.isEmpty();
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

}
