/*
 * Copyright [ 2024 - 2026 ] MapsMessaging B.V.
 */
package io.mapsmessaging.storage.impl.expired;

import io.mapsmessaging.storage.ExpiredMonitor;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.impl.file.TaskQueue;
import io.mapsmessaging.storage.impl.file.tasks.FileTask;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ExpiryLifecycleTest {
  @Test
  void pauseCancelsPendingScanAndResumeSchedulesExactlyOneReplacement() throws Exception {
    ManualQueue queue = new ManualQueue();
    AtomicInteger scans = new AtomicInteger();
    try (ExpireStorableTaskManager<Entry> manager = new ExpireStorableTaskManager<>(scans::incrementAndGet, queue, 1)) {
      manager.added(new Entry(1));
      manager.schedulePoll();
      assertEquals(1, queue.tasks.size());
      manager.pause();
      manager.pause();
      queue.tasks.get(0).run();
      assertEquals(0, scans.get());
      manager.resume();
      manager.resume();
      assertEquals(2, queue.tasks.size());
      queue.tasks.get(1).run();
      assertEquals(1, scans.get());
    }
  }

  @Test
  void closePreventsQueuedAndNewScans() throws Exception {
    ManualQueue queue = new ManualQueue();
    AtomicInteger scans = new AtomicInteger();
    ExpireStorableTaskManager<Entry> manager = new ExpireStorableTaskManager<>(scans::incrementAndGet, queue, 1);
    manager.added(new Entry(1));
    manager.close();
    manager.added(new Entry(1));
    manager.schedulePoll();
    queue.tasks.forEach(FutureTask::run);
    assertEquals(0, scans.get());
    assertEquals(1, queue.tasks.size());
  }

  @Test
  void resumeWithoutExpiringEntriesDoesNotStartMonitoring() throws Exception {
    ManualQueue queue = new ManualQueue();
    try (ExpireStorableTaskManager<Entry> manager = new ExpireStorableTaskManager<>(() -> fail("Unexpected scan"), queue, 1)) {
      manager.pause();
      manager.added(new Entry(0));
      manager.resume();
      assertTrue(queue.tasks.isEmpty());
    }
  }

  @Test
  void additionDuringScanSchedulesAnotherPollDespiteEmptyScanResult() throws Exception {
    ManualQueue queue = new ManualQueue();
    AtomicInteger scans = new AtomicInteger();
    List<ExpireStorableTaskManager<Entry>> managers = new ArrayList<>();
    ExpiredMonitor monitor = new ExpiredMonitor() {
      public void scanForExpired() {
        if (scans.incrementAndGet() == 1) managers.get(0).added(new Entry(1));
      }
      public boolean hasExpiringEntries() { return false; }
    };
    try (ExpireStorableTaskManager<Entry> manager = new ExpireStorableTaskManager<>(monitor, queue, 1)) {
      managers.add(manager);
      manager.added(new Entry(1));
      queue.tasks.get(0).run();
      assertEquals(2, queue.tasks.size());
      queue.tasks.get(1).run();
      assertEquals(2, scans.get());
      assertEquals(2, queue.tasks.size());
    }
  }

  private record Entry(long expiry) implements Storable {
    public long getKey() { return 1; }
    public long getExpiry() { return expiry; }
  }

  private static class ManualQueue extends TaskQueue {
    final List<FutureTask<?>> tasks = new ArrayList<>();
    @Override
    public <V> Future<V> schedule(FileTask<V> task, long delay, TimeUnit unit) {
      assertEquals(1, delay);
      assertEquals(TimeUnit.SECONDS, unit);
      FutureTask<V> future = new FutureTask<>(task);
      tasks.add(future);
      return future;
    }
  }
}
