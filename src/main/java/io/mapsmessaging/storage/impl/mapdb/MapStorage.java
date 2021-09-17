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

package io.mapsmessaging.storage.impl.mapdb;

import io.mapsmessaging.storage.Factory;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.jetbrains.annotations.NotNull;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

public class MapStorage<T extends Storable> implements Storage<T> {

  private final BTreeMap<Long, T> diskMap;
  private final DB dataStore;
  private final String fileName;
  private final boolean sync;
  private volatile boolean isClosed;

  public MapStorage(String fileName, String name, Factory<T> factory, boolean sync) {
    this.fileName = fileName;
    dataStore = DBMaker.fileDB(fileName)
        .fileMmapEnable()
        .closeOnJvmShutdown()
        .cleanerHackEnable()
        .checksumHeaderBypass()
        .make();
    diskMap = dataStore
        .treeMap(name, Serializer.LONG, new MapSerializer<>(factory))
        .createOrOpen();
    isClosed = false;
    this.sync = sync;
  }

  @Override
  public boolean isEmpty() {
    return diskMap.isEmpty();
  }

  @Override
  public @NotNull List<Long> keepOnly(@NotNull List<Long> listToKeep) throws IOException {
    Set<Long> itemsToRemove = diskMap.keySet();
    itemsToRemove.removeIf(listToKeep::contains);
    if (!itemsToRemove.isEmpty()) {
      for (long key : itemsToRemove) {
        diskMap.remove(key);
      }
    }

    if (itemsToRemove.size() != listToKeep.size()) {
      Set<Long> actual = diskMap.keySet();
      listToKeep.removeIf(actual::contains);
      return listToKeep;
    }
    return new ArrayList<>();
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
  public synchronized void add(@NotNull T obj) throws IOException {
    diskMap.put(obj.getKey(), obj);
    if (sync) {
      dataStore.commit();
    }
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
    if (sync) {
      dataStore.commit();
    }
    return true;
  }

  @Override
  public synchronized long size() {
    return diskMap.size();
  }

}