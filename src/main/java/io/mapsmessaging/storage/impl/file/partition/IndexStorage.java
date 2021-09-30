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

package io.mapsmessaging.storage.impl.file.partition;

import io.mapsmessaging.storage.Factory;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.storage.impl.file.TaskQueue;
import io.mapsmessaging.storage.impl.file.tasks.CompactIndexTask;
import io.mapsmessaging.storage.impl.file.tasks.ValidateIndexAndDataTask;
import io.mapsmessaging.utilities.threads.tasks.TaskScheduler;
import java.nio.file.StandardCopyOption;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import static java.nio.file.StandardOpenOption.*;

public class IndexStorage<T extends Storable> implements Storage<T> {

  private static final double VERSION = 1.0;
  private static final long UNIQUE_ID = 0xf00d0000d00f0000L;
  private static final long OPEN_STATE = 0xEFFFFFFFFFFFFFFFL;
  private static final long CLOSE_STATE = 0x0000000000000000L;

  private static final int ITEM_COUNT = 524288;

  private IndexManager indexManager;
  private FileChannel mapChannel;

  private final boolean sync;
  private final String fileName;
  private final DataStorage<T> dataStorage;

  private final TaskQueue scheduler;
  private volatile boolean closed;
  private boolean requiresValidation;

  public IndexStorage(String fileName, Factory<T> factory, boolean sync, long start, TaskQueue taskScheduler) throws IOException {
    this.fileName = fileName+"_index";
    File file = new File(this.fileName);
    scheduler = taskScheduler;
    this.sync = sync;
    long length = 0;
    if (file.exists()) {
      length = file.length();
    }
    StandardOpenOption[] writeOptions;
    if (sync) {
      writeOptions = new StandardOpenOption[]{CREATE, READ, WRITE, SPARSE, DSYNC};
    }
    else{
      writeOptions = new StandardOpenOption[]{CREATE, READ, WRITE, SPARSE};
    }

    mapChannel = (FileChannel) Files.newByteChannel(file.toPath(), writeOptions);
    if(length != 0){
      reload();
    }
    else{
      initialise(start);
    }
    dataStorage = new DataStorage<>(fileName+"_data", factory, sync);
    if(dataStorage.isValidationRequired() || requiresValidation){
      scheduler.submit(new ValidateIndexAndDataTask<>(this));
    }
    closed = false;
  }

  @Override
  public void close() throws IOException {
    if(!closed) {
      closed = true;
      ByteBuffer header = ByteBuffer.allocate(8);
      header.putLong(CLOSE_STATE);
      mapChannel.write(header);
      indexManager.close();
      mapChannel.force(true);
      mapChannel.close();
      dataStorage.close();
    }
  }

  private void initialise(long start) throws IOException {
    ByteBuffer headerValidation = ByteBuffer.allocate(32);
    headerValidation.putLong(OPEN_STATE);
    headerValidation.putLong(UNIQUE_ID);
    headerValidation.putLong(Double.doubleToLongBits(VERSION));
    headerValidation.putLong(ITEM_COUNT);
    headerValidation.flip();
    mapChannel.write(headerValidation);
    indexManager = new IndexManager(start, ITEM_COUNT, mapChannel);
    mapChannel.force(false);
    requiresValidation = false;
  }

  private void reload()throws IOException{
    ByteBuffer headerValidation = ByteBuffer.allocate(32);
    mapChannel.read(headerValidation);
    headerValidation.flip();
    requiresValidation = headerValidation.getLong() != CLOSE_STATE;
    if(headerValidation.getLong() != UNIQUE_ID){
      throw new IOException("Unexpected file identifier located");
    }
    if(Double.longBitsToDouble(headerValidation.getLong()) != VERSION){
      throw new IOException("Unexpected file version");
    }
    if(headerValidation.getLong() != ITEM_COUNT){
      throw new IOException("Unexpected item count");
    }
    indexManager = new IndexManager(mapChannel);

    headerValidation.flip();
    headerValidation.putLong(0,OPEN_STATE);
    mapChannel.position(0);
    mapChannel.write(headerValidation);
    mapChannel.force(false);
  }

  public void compact() throws IOException {
    long size = ((indexManager.getEnd() - indexManager.getStart() + 1) * IndexRecord.HEADER_SIZE) + 24 + 16;
    if(size < mapChannel.size()){
      File currentIndex = new File(fileName);
      File tmpIndex = new File(fileName+"_tmp");
      try (FileChannel tmp = (FileChannel) Files.newByteChannel(tmpIndex.toPath(), CREATE_NEW, WRITE)){
        mapChannel.position(0);
        long moved = tmp.transferFrom(mapChannel, 0, size);
        if(moved != size){
          if(!tmpIndex.delete()){
            //ToDo log this for later info
          }
          throw new IOException("Unable to compact index");
        }
        tmp.force(true);
      }
      indexManager.close();
      mapChannel.force(true);
      mapChannel.close();
      Files.copy(tmpIndex.toPath(), currentIndex.toPath(), StandardCopyOption.REPLACE_EXISTING);
      if(!tmpIndex.delete()){
        // ToDo log this for later
      }

      StandardOpenOption[] writeOptions;
      if (sync) {
        writeOptions = new StandardOpenOption[]{CREATE, READ, WRITE, SPARSE, DSYNC};
      }
      else{
        writeOptions = new StandardOpenOption[]{CREATE, READ, WRITE, SPARSE};
      }
      mapChannel = (FileChannel) Files.newByteChannel(currentIndex.toPath(), writeOptions);
      reload();
    }
  }

  public long getStart(){
    return indexManager.getStart();
  }

  public long getEnd(){
    return indexManager.getEnd();
  }

  public void setEnd(long key) throws IOException {
    indexManager.setEnd(key);
    scheduler.submit(new CompactIndexTask<>(this));
  }

  @Override
  public String getName() {
    return fileName;
  }

  @Override
  public void delete() throws IOException {
    indexManager.close();
    mapChannel.close();
    dataStorage.delete();
    File path = new File(fileName);
    Files.delete(path.toPath());
  }

  @Override
  public void add(@NotNull T object) throws IOException {
    IndexRecord item = dataStorage.add(object);
    indexManager.add(object.getKey(), item);
  }

  public boolean isFull(){
    return dataStorage.isFull();
  }

  @Override
  public boolean remove(long key) throws IOException {
    return indexManager.delete(key);
  }

  @Override
  public @Nullable T get(long key) throws IOException {
    T obj = null;
    if (key >= 0) {
      IndexRecord item = indexManager.get(key);
      if(item != null){
        obj = dataStorage.get(item);
      }
    }
    return obj;
  }

  @Override
  public long size() throws IOException {
    return indexManager.size();
  }

  @Override
  public boolean isEmpty() {
    return indexManager.size() == 0;
  }

  @Override
  public @NotNull List<Long> keepOnly(@NotNull List<Long> listToKeep) throws IOException {
    List<Long> itemsToRemove = indexManager.keySet();
    itemsToRemove.removeIf(listToKeep::contains);
    if (!itemsToRemove.isEmpty()) {
      for (long key : itemsToRemove) {
        remove(key);
      }
    }

    if (itemsToRemove.size() != listToKeep.size()) {
      List<Long> actual = indexManager.keySet();
      listToKeep.removeIf(actual::contains);
      return listToKeep;
    }
    return new ArrayList<>();
  }

  @Override
  public void setExecutor(TaskScheduler scheduler) {
    // We don't actually get the executor here
  }
}