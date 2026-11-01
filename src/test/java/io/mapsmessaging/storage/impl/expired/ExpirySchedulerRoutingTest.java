package io.mapsmessaging.storage.impl.expired;

import io.mapsmessaging.storage.ExpiredMonitor;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.impl.file.TaskQueue;
import io.mapsmessaging.storage.impl.file.tasks.FileTask;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

class ExpirySchedulerRoutingTest {

  @Test
  void expiryTimerSubmitsWorkWithoutRunningTheScanOnTheTimerThread() throws Exception {
    CapturingTaskQueue queue = new CapturingTaskQueue();
    AtomicInteger scans = new AtomicInteger();
    ExpiredMonitor monitor = new ExpiredMonitor() {
      @Override
      public void scanForExpired() {
        scans.incrementAndGet();
      }

      @Override
      public boolean hasExpiringEntries() {
        return false;
      }
    };

    try (ExpireStorableTaskManager<TestStorable> manager =
             new ExpireStorableTaskManager<>(monitor, queue, 1)) {
      manager.added(new TestStorable(7L, System.currentTimeMillis() + 10_000));
      Assertions.assertTrue(queue.submitted.await(3, TimeUnit.SECONDS));
      Assertions.assertEquals(0, scans.get(), "Timer must not run expiry scan");
      FileTask<?> pending = queue.captured.get();
      Assertions.assertNotNull(pending);
      pending.call();
      Assertions.assertEquals(1, scans.get());
    }
  }

  private static final class CapturingTaskQueue extends TaskQueue {
    @Override
    public boolean hasExecutor() {
      return true;
    }

    private final CountDownLatch submitted = new CountDownLatch(1);
    private final AtomicReference<FileTask<?>> captured = new AtomicReference<>();

    @Override
    public void submit(FileTask<?> task) throws IOException {
      captured.set(task);
      submitted.countDown();
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
