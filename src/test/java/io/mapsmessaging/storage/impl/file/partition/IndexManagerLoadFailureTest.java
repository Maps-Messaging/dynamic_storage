package io.mapsmessaging.storage.impl.file.partition;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.junit.jupiter.api.Assertions.*;

class IndexManagerLoadFailureTest {

  @Test
  void successfulLoadMakesIndexAvailable(@TempDir Path directory) throws Exception {
    try (FileChannel channel = FileChannel.open(directory.resolve("successful"), CREATE, READ, WRITE);
         IndexManager manager = new IndexManager(0, 2, channel)) {
      manager.loadMap(false);
      assertEquals(0, manager.size());
      assertFalse(manager.contains(1));
    }
  }

  @Test
  void failedLoadReportsCauseToWaitingCaller(@TempDir Path directory) throws Exception {
    try (FileChannel channel = FileChannel.open(directory.resolve("failed"), CREATE, READ, WRITE)) {
      RuntimeException failure = new IllegalArgumentException("bad index");
      CountDownLatch scanning = new CountDownLatch(1);
      CountDownLatch release = new CountDownLatch(1);
      IndexManager manager = new IndexManager(0, 2, channel) {
        @Override
        List<Long> walkIndex() {
          scanning.countDown();
          try {
            if (!release.await(5, TimeUnit.SECONDS)) {
              throw new IllegalStateException("test timed out");
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
          }
          throw failure;
        }
      };
      ExecutorService executor = Executors.newFixedThreadPool(2);
      try {
        Future<?> loader = executor.submit(() -> manager.loadMap(true));
        assertTrue(scanning.await(5, TimeUnit.SECONDS));
        Future<Integer> waiter = executor.submit(manager::size);
        assertFalse(waiter.isDone(), "Waiting caller must not return while index is loading");
        release.countDown();

        java.util.concurrent.ExecutionException loaderError =
            assertThrows(java.util.concurrent.ExecutionException.class, () -> loader.get(5, TimeUnit.SECONDS));
        assertSame(failure, loaderError.getCause());

        java.util.concurrent.ExecutionException waitingError =
            assertThrows(java.util.concurrent.ExecutionException.class, () -> waiter.get(5, TimeUnit.SECONDS));
        assertInstanceOf(IllegalStateException.class, waitingError.getCause());
        assertSame(failure, waitingError.getCause().getCause());
        IllegalStateException subsequent = assertThrows(IllegalStateException.class, () -> manager.contains(0));
        assertSame(failure, subsequent.getCause());
      } finally {
        release.countDown();
        executor.shutdownNow();
      }
    }
  }
}
