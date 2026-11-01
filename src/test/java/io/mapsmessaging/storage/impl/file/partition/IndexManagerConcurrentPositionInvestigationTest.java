/*
 *
 * Copyright [ 2024 - 2026 ] MapsMessaging B.V.
 *
 * Licensed under the Apache License, Version 2.0 with the Commons Clause
 * (the "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *     https://commonsclause.com/
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package io.mapsmessaging.storage.impl.file.partition;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

class IndexManagerConcurrentPositionInvestigationTest {

  @Test
  void simultaneousReadsMustNotReadMetadataFromTheOtherIndexSlot(@TempDir Path directory) throws Exception {
    try (FileChannel channel = FileChannel.open(directory.resolve("parallel-index"), CREATE, READ, WRITE);
         CoordinatedIndexManager manager = new CoordinatedIndexManager(0L, 2, channel)) {
      manager.loadMap(false);
      Assertions.assertTrue(manager.add(0L, new IndexRecord(0L, 1, 101L, 0L, 24)));
      Assertions.assertTrue(manager.add(1L, new IndexRecord(1L, 1, 202L, 0L, 24)));
      Assertions.assertEquals(101L, manager.get(0L).getPosition());
      Assertions.assertEquals(202L, manager.get(1L).getPosition());

      // Force both workers to reach the interval between positioning the
      // shared buffer and reading a relative IndexRecord from that buffer.
      ExecutorService executor = Executors.newFixedThreadPool(2);
      try {
        manager.coordinateReads();
        Future<Long> first = executor.submit(() -> manager.get(0L).getPosition());
        Future<Long> second = executor.submit(() -> manager.get(1L).getPosition());
        try {
          Assertions.assertAll("Concurrent reads must retain their own index positions",
              () -> Assertions.assertEquals(101L, first.get(5, TimeUnit.SECONDS)),
              () -> Assertions.assertEquals(202L, second.get(5, TimeUnit.SECONDS)));
        } finally {
          manager.endCoordination();
        }
      } finally {
        executor.shutdownNow();
      }
    }
  }

  @Test
  void simultaneousWriteAndReadMustUseIndependentIndexPositions(@TempDir Path directory) throws Exception {
    try (FileChannel channel = FileChannel.open(directory.resolve("parallel-write-read"), CREATE, READ, WRITE);
         CoordinatedIndexManager manager = new CoordinatedIndexManager(0L, 2, channel)) {
      manager.loadMap(false);
      Assertions.assertTrue(manager.add(1L, new IndexRecord(1L, 1, 202L, 0L, 24)));

      ExecutorService executor = Executors.newFixedThreadPool(2);
      try {
        manager.coordinateReads();
        Future<Boolean> writer = executor.submit(() -> manager.add(0L, new IndexRecord(0L, 1, 101L, 0L, 24)));
        Future<Long> reader = executor.submit(() -> manager.get(1L).getPosition());
        try {
          Assertions.assertAll("Concurrent index writes and reads must retain independent positions",
              () -> Assertions.assertTrue(writer.get(5, TimeUnit.SECONDS)),
              () -> Assertions.assertEquals(202L, reader.get(5, TimeUnit.SECONDS)));
        } finally {
          manager.endCoordination();
        }
      } finally {
        executor.shutdownNow();
      }

      Assertions.assertEquals(101L, manager.get(0L).getPosition());
      Assertions.assertEquals(202L, manager.get(1L).getPosition());
    }
  }

  private static final class CoordinatedIndexManager extends IndexManager {

    private final CyclicBarrier bothPositioned = new CyclicBarrier(2);
    private volatile boolean coordinate;

    CoordinatedIndexManager(long start, int count, FileChannel channel) throws IOException {
      super(start, count, channel);
    }

    void coordinateReads() {
      coordinate = true;
    }

    void endCoordination() {
      coordinate = false;
    }

    @Override
    ByteBuffer recordView(long key) {
      ByteBuffer view = super.recordView(key);
      if (coordinate) {
        try {
          bothPositioned.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException("Interrupted while coordinating index reads", e);
        } catch (BrokenBarrierException | TimeoutException e) {
          throw new IllegalStateException("Unable to coordinate parallel index reads", e);
        }
      }
      return view;
    }
  }
}
