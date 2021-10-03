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
import io.mapsmessaging.storage.tasks.Completion;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.jetbrains.annotations.NotNull;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;

@State(Scope.Benchmark)
public class SimpleBenchmark extends BaseTest {

  public boolean readWriteQueues = true;

  public boolean enableSync = false;

  //@Param({"JCS", "WeakReference"})
  public String enableCache = "JCS";

  //@Param({"e:\\jmh\\", "m:\\jmh\\", "c:\\jmh\\", "d:\\jmh\\"})
  public String drive = "m:\\jmh\\";

  private static final int STORE_SIZE = 100;
  static{
    System.setProperty("PoolDepth", ""+STORE_SIZE*2);
  }
  Queue<Long>[] queue = new Queue[STORE_SIZE];
  AsyncStorage<BufferedData>[] storageArray = new AsyncStorage[STORE_SIZE];
  AtomicLong[] id = new AtomicLong[STORE_SIZE];
  AtomicLong indexer = new AtomicLong(0);


  @Setup
  public void createState() throws IOException {
    System.err.println("Creating new stores :: ReadWriteQueues:"+readWriteQueues+" Sync:"+enableSync+" Cache:"+enableCache+" Drive:"+drive);
    for(int x=0;x<storageArray.length;x++) {
      queue[x] = new ConcurrentLinkedDeque<>();
      File store = new File(drive+"FileTest2_"+x);
      if (store.exists()) {
        Files.delete(store.toPath());
      }
      Map<String, String> properties = new LinkedHashMap<>();
      properties.put("Sync", ""+enableSync);
      StorageBuilder<BufferedData> storageBuilder = new StorageBuilder<>();
      storageBuilder.setStorageType("SeekableChannel")
          .setFactory(new DataFactory())
          .setName(drive+"FileTest2_"+x)
          .setCache(enableCache)
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
    System.err.println("Deleted stores");
  }

  @Benchmark
  @BenchmarkMode({Mode.All})
  @Fork(value = 1, warmups = 2)
  @Threads(2000)
  public void performTasks() throws IOException, ExecutionException, InterruptedException {
    int idx = (int)(indexer.incrementAndGet() % STORE_SIZE);
    Long test = queue[idx].poll();
    while(test != null){
      Future<BufferedData> future = storageArray[idx].get(test, new GetCompletion(storageArray[idx]));
      future.get();
      test = queue[idx].poll();
    }
    long key = id[idx].incrementAndGet();
    Future<BufferedData> future = storageArray[idx].add(new BufferedData(createMessageBuilder(key)), null);
    future.get();
    queue[idx].offer(key);
  }

   static final class GetCompletion implements Completion<BufferedData> {

    private final AsyncStorage<BufferedData> storage;

    GetCompletion(AsyncStorage<BufferedData> storage){
      this.storage = storage;
    }
    @Override
    public void onCompletion(BufferedData result) {
      try {
        if(result == null){
          System.err.println("Data discrepancy");
          System.exit(1);
        }
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

    public BufferedData(MappedData data){
      this.data = data;
    }

    public BufferedData(@NotNull ByteBuffer[] buffers) throws IOException {
      this.data = new MappedData(buffers);
    }

    @Override
    public long getKey() {
      return data.key;
    }

    @Override
    public long getExpiry() {
      return 0;
    }

    @Override
    public @NotNull ByteBuffer[] write() throws IOException {
      return data.write();
    }
  }

  static class DataFactory implements Factory<BufferedData>{


    @Override
    public BufferedData create(ByteBuffer[] buffers) throws IOException {
      return new BufferedData(buffers);
    }
  }

}
