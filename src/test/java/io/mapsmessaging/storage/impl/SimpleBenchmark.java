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

package io.mapsmessaging.storage.impl;

import io.mapsmessaging.storage.AsyncStorage;
import io.mapsmessaging.storage.Factory;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.StorageBuilder;
import io.mapsmessaging.storage.impl.streams.BufferObjectReader;
import io.mapsmessaging.storage.impl.streams.BufferObjectWriter;
import io.mapsmessaging.storage.impl.streams.ObjectReader;
import io.mapsmessaging.storage.impl.streams.ObjectWriter;
import io.mapsmessaging.storage.impl.streams.StreamObjectReader;
import io.mapsmessaging.storage.impl.streams.StreamObjectWriter;
import io.mapsmessaging.storage.tasks.Completion;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import org.jetbrains.annotations.NotNull;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;

@State(Scope.Benchmark)
public class SimpleBenchmark extends BaseTest {

  private static final int STORE_SIZE = 100;
  static{
    System.setProperty("PoolDepth", ""+STORE_SIZE*2);
  }

  AsyncStorage<BufferedData>[] storageArray = new AsyncStorage[STORE_SIZE];
  AtomicLong[] id = new AtomicLong[STORE_SIZE];
  AtomicLong indexer = new AtomicLong(0);

  @Setup
  public void createState() throws IOException {
    for(int x=0;x<storageArray.length;x++) {
      System.err.println("New Queue created");
      File store = new File("FileTest2_"+x);
      if (store.exists()) {
        Files.delete(store.toPath());
      }
      Map<String, String> properties = new LinkedHashMap<>();
      properties.put("Sync", "false");
      StorageBuilder<BufferedData> storageBuilder = new StorageBuilder<>();
      storageBuilder.setStorageType("File")
          .setFactory(new DataFactory())
          .setName("FileTest2_"+x)
          .setProperties(properties);
      storageArray[x] = storageBuilder.buildAsync();
      id[x] = new AtomicLong(0);
    }
  }

  @TearDown
  public void cleanUp() throws IOException, ExecutionException, InterruptedException {
    for (AsyncStorage<BufferedData> storage : storageArray) {
      storage.delete(null).get();
    }
    System.err.println("Deleted store");
  }

  @Benchmark
  @BenchmarkMode({Mode.All})
  @Fork(value = 1, warmups = 2)
  @Threads(2000)
  public void performTasks() throws IOException, ExecutionException, InterruptedException {
    int idx = (int)(indexer.incrementAndGet() % STORE_SIZE);
    long key = id[idx].incrementAndGet();
    storageArray[idx].add(new BufferedData(createMessageBuilder(key)), new AddCompletion(storageArray[idx])).get();
  }

  static final class AddCompletion implements Completion<BufferedData> {

    private final AsyncStorage<BufferedData> storage;

    AddCompletion(AsyncStorage<BufferedData> storage){
      this.storage = storage;
    }
    @Override
    public void onCompletion(BufferedData result) {
      try {
        storage.get(result.getKey(), new GetCompletion(storage));
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    @Override
    public void onException(Exception exception) {
      exception.printStackTrace();
      System.exit(1);
    }
  }

  static final class GetCompletion implements Completion<BufferedData> {

    private final AsyncStorage<BufferedData> storage;

    GetCompletion(AsyncStorage<BufferedData> storage){
      this.storage = storage;
    }
    @Override
    public void onCompletion(BufferedData result) {
      try {
        storage.remove(result.getKey(), new RemoveCompletion());
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    @Override
    public void onException(Exception exception) {
      exception.printStackTrace();
      System.exit(1);
    }
  }

  static final class RemoveCompletion implements Completion<Boolean> {

    @Override
    public void onCompletion(Boolean result) {
    }

    @Override
    public void onException(Exception exception) {
      exception.printStackTrace();
      System.exit(1);
    }
  }

  static class BufferedData implements Storable{

    private final MappedData data;

    public BufferedData(){
      this.data = new MappedData();
    }

    public BufferedData(MappedData data){
      this.data = data;
    }

    @Override
    public long getKey() {
      return data.key;
    }

    @Override
    public void read(@NotNull ObjectReader objectReader) throws IOException {
      ByteBuffer bb = ByteBuffer.allocate(10240);
      byte[] buffer = objectReader.readByteArray();
      bb.put(buffer);
      bb.flip();
      BufferObjectReader bor = new BufferObjectReader(bb);
      data.readHeader(bor);
      data.readMap(bor);
      data.readData(objectReader);
    }

    @Override
    public void write(@NotNull ObjectWriter objectWriter) throws IOException {
      ByteBuffer bb = ByteBuffer.allocate(10240);
      BufferObjectWriter bow = new BufferObjectWriter(bb);
      data.writeHeader(bow);
      data.writeMap(bow);
      int size = bb.position();
      byte[] t =  new byte[size];
      bb.flip();
      bb.get(t);
      objectWriter.write(t);
      data.writeData(objectWriter);
    }
  }

  static class DataFactory implements Factory<BufferedData>{


    @Override
    public BufferedData create() {
      return new BufferedData();
    }
  }

}
