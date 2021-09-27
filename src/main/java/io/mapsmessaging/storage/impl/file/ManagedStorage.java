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
import io.mapsmessaging.storage.impl.file.tasks.ValidateIndexAndDataTask;
import io.mapsmessaging.utilities.threads.tasks.PriorityTaskScheduler;
import io.mapsmessaging.utilities.threads.tasks.TaskScheduler;
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
  private final String fileName;
  private final FileChannel mapChannel;
  private final DataStorage<T> dataStorage;

  private TaskScheduler scheduler;
  private volatile boolean closed;
  private boolean requiresValidation;

  public ManagedStorage(String fileName, Factory<T> factory, boolean sync) throws IOException {
    this.fileName = fileName+"_index";
    File file = new File(this.fileName);
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
      initialise();
    }
    dataStorage = new DataStorage<T>(fileName+"_data", factory, sync);
    closed = false;
    scheduler = null;
  }

  @Override
  public void close() throws IOException {
    if(!closed) {
      closed = true;
      ByteBuffer header = ByteBuffer.allocate(8);
      header.putLong(CLOSE_STATE);
      mapChannel.write(header);
      headerManager.close();
      mapChannel.force(true);
      mapChannel.close();
      dataStorage.close();
    }
  }

  private void initialise() throws IOException {
    ByteBuffer headerValidation = ByteBuffer.allocate(32);
    headerValidation.putLong(OPEN_STATE);
    headerValidation.putLong(UNIQUE_ID);
    headerValidation.putLong(Double.doubleToLongBits(VERSION));
    headerValidation.putLong(ITEM_COUNT);
    headerValidation.flip();
    mapChannel.write(headerValidation);
    headerManager= new HeaderManager(0L, ITEM_COUNT, mapChannel);
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
    headerManager = new HeaderManager(mapChannel);

    headerValidation.flip();
    headerValidation.putLong(0,OPEN_STATE);
    mapChannel.position(0);
    mapChannel.write(headerValidation);
    mapChannel.force(false);
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
    dataStorage.delete();
  }

  @Override
  public void add(@NotNull T object) throws IOException {
    HeaderItem item = dataStorage.add(object);
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
        obj = dataStorage.get(item);
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

  @Override
  public void setTaskQueue(TaskScheduler scheduler) {
    this.scheduler = scheduler;
    if(requiresValidation){
      scheduler.submit(new ValidateIndexAndDataTask<Void, T>(this)); // Will be submitted at low priority
    }
  }

}
