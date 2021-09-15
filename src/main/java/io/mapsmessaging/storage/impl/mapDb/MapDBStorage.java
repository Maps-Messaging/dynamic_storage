package io.mapsmessaging.storage.impl.mapDb;

import io.mapsmessaging.storage.Factory;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

public class MapDBStorage<T extends Storable> implements Storage<T> {


  private volatile boolean isClosed;

  private final Factory<T> factory;
  private final BTreeMap<Long, T> diskMap;
  private final DB dataStore;
  private final String fileName;

  public MapDBStorage(String fileName, String name, Factory<T> factory) {
    this.fileName = fileName;
    this.factory = factory;
    dataStore = DBMaker.fileDB(fileName)
        .fileMmapEnable()
        .closeOnJvmShutdown()
        .cleanerHackEnable()
        .checksumHeaderBypass()
        .make();
    diskMap = dataStore
        .treeMap(name, Serializer.LONG, new MapDBSerializer<>(factory))
        .createOrOpen();
    isClosed = false;

  }

  @Override
  public boolean isEmpty() {
    return diskMap.isEmpty();
  }

  @Override
  public @NotNull List<Long> keepOnly(@NotNull List<Long> listToKeep) throws IOException {
    return null;
  }

  @Override
  public String getName() {
    return fileName;
  }

  @Override
  public synchronized void delete() throws IOException {
    close();
    File tmp = new File(fileName);
    Files.delete(tmp.toPath());
  }

  @Override
  public synchronized void close() throws IOException {
    if (!isClosed) {
      isClosed = true;
      dataStore.commit();
      dataStore.close();
    }
  }

  @Override
  public synchronized void add(T obj) throws IOException {
    diskMap.put(obj.getKey(), obj);
  }

  @Override
  public synchronized T get(long key) {
    T obj = null;
    if (key >= 0) {
      obj = diskMap.get(key);
    }
    return obj;
  }

  @Override
  public synchronized boolean remove(long key) throws IOException {
    diskMap.remove(key);
    return true;
  }

  @Override
  public synchronized long size() {
    return diskMap.size();
  }

}