package io.mapsmessaging.storage.impl.expired;

import io.mapsmessaging.storage.ExpiredMonitor;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.impl.file.TaskQueue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

class ExpirySchedulerRoutingTest {

  @Test
  void expiryTimerRunsScanDirectlyWithoutSubmittingToStorageExecutor() throws Exception {
    CapturingTaskQueue queue = new CapturingTaskQueue();
    AtomicInteger scans = new AtomicInteger();
    CountDownLatch scanned = new CountDownLatch(1);
    ExpiredMonitor monitor = new ExpiredMonitor() {
      @Override
      public void scanForExpired() {
        scans.incrementAndGet();
        scanned.countDown();
      }

      @Override
      public boolean hasExpiringEntries() {
        return false;
      }
    };

    try (ExpireStorableTaskManager<TestStorable> manager =
             new ExpireStorableTaskManager<>(monitor, queue, 1)) {
      manager.added(new TestStorable(7L, System.currentTimeMillis() + 10_000));
      Assertions.assertTrue(scanned.await(3, TimeUnit.SECONDS));
      Assertions.assertEquals(1, scans.get());
      Assertions.assertEquals(0, queue.submissions.get(), "Timer should run scan directly");
    }
  }

  private static final class CapturingTaskQueue extends TaskQueue {
    private final AtomicInteger submissions = new AtomicInteger();

    @Override
    public void submit(io.mapsmessaging.storage.impl.file.tasks.FileTask<?> task) {
      submissions.incrementAndGet();
    }
  }

  private record TestStorable(long key, long expiry) implements Storable {
    @Override
    public long getKey() {
      return key;
    }

    @Override
    public long getExpiry() {
      return expiry;
    }
  }
}
