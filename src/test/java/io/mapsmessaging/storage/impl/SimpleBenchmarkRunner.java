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

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import lombok.SneakyThrows;

public class SimpleBenchmarkRunner {

  private static final int LOOP_COUNT = 1000000;
  private static final int THREAD_COUNT = 10;



  public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
    SimpleBenchmark benchmark = new SimpleBenchmark();
    benchmark.createState();
    CountDownLatch countDownLatch = new CountDownLatch(THREAD_COUNT);
    for(int x=0;x<THREAD_COUNT;x++){
      Runner r = new Runner(benchmark, countDownLatch);
      Thread t = new Thread(r);
      t.start();
    }
    countDownLatch.await();
    benchmark.cleanUp();
  }

  public static class Runner implements Runnable{
    private final CountDownLatch countDownLatch;
    private final SimpleBenchmark simpleBenchmark;
    public Runner(SimpleBenchmark simpleBenchmark, CountDownLatch countDownLatch){
      this.simpleBenchmark = simpleBenchmark;
      this.countDownLatch = countDownLatch;
    }

    @SneakyThrows
    @Override
    public void run() {
      try {
        for(int x=0;x<LOOP_COUNT;x++){
          simpleBenchmark.performTasks();
        }
      } finally {
        countDownLatch.countDown();
      }
    }
  }
}
