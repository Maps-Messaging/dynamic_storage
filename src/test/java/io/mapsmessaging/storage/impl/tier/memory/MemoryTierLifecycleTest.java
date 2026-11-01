package io.mapsmessaging.storage.impl.tier.memory;

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class MemoryTierLifecycleTest {
  private record Entry(long key) implements Storable {
    @Override public long getKey() { return key; }
    @Override public long getExpiry() { return 0; }
  }

  private static class Store<T extends Storable> {
    final Map<Long, T> records = new LinkedHashMap<>();
    final CountDownLatch entered = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final AtomicBoolean blocked = new AtomicBoolean();
    final AtomicBoolean terminated = new AtomicBoolean();

    @SuppressWarnings("unchecked")
    Storage<T> proxy() {
      return (Storage<T>) Proxy.newProxyInstance(Storage.class.getClassLoader(), new Class<?>[]{Storage.class},
          (proxy, method, args) -> switch (method.getName()) {
            case "add" -> {
              if (blocked.get()) {
                entered.countDown();
                if (!release.await(10, TimeUnit.SECONDS)) throw new IOException("Test write timeout");
              }
              T entry = (T) args[0];
              synchronized (records) { records.put(entry.getKey(), entry); }
              yield null;
            }
            case "get" -> { synchronized (records) { yield records.get((Long) args[0]); } }
            case "getKeys" -> { synchronized (records) { yield List.copyOf(records.keySet()); } }
            case "remove" -> { synchronized (records) { yield records.remove((Long) args[0]) != null; } }
            case "close", "delete" -> { terminated.set(true); yield null; }
            case "getName" -> "test-tier";
            default -> throw new UnsupportedOperationException(method.getName());
          });
    }
  }

  private void assertWaitsForScan(boolean delete) throws Exception {
    Store<ObjectMonitor<Entry>> memory = new Store<>();
    Store<Entry> disk = new Store<>();
    MemoryTierConfig config = new MemoryTierConfig();
    config.setMigrationTime(-1000);
    MemoryTierStorage<Entry> tier = new MemoryTierStorage<>(memory.proxy(), disk.proxy(), config);
    tier.add(new Entry(1));
    tier.add(new Entry(2));
    disk.blocked.set(true);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread scan = new Thread(() -> {
      try { tier.scan(); } catch (Throwable e) { failure.compareAndSet(null, e); }
    });
    scan.start();
    assertTrue(disk.entered.await(2, TimeUnit.SECONDS));
    Thread lifecycle = new Thread(() -> {
      try {
        if (delete) tier.delete(); else tier.close();
      } catch (Throwable e) {
        failure.compareAndSet(null, e);
      }
    });
    lifecycle.start();
    try {
      assertFalse(disk.terminated.get(), "Persistent storage closed during active write");
      disk.release.countDown();
      scan.join(3000);
      lifecycle.join(3000);
      assertFalse(scan.isAlive());
      assertFalse(lifecycle.isAlive());
      assertNull(failure.get());
      assertTrue(disk.terminated.get());
    } finally {
      disk.release.countDown();
      scan.join(3000);
      lifecycle.join(3000);
    }
  }

  @Test void closeWaitsForCurrentMigrationWrite() throws Exception {
    assertWaitsForScan(false);
  }

  @Test void deleteWaitsForCurrentMigrationWrite() throws Exception {
    assertWaitsForScan(true);
  }

  @Test void shutdownTimesOutWithoutClosingTiersDuringBlockedMigration() throws Exception {
    Store<ObjectMonitor<Entry>> memory = new Store<>();
    Store<Entry> disk = new Store<>();
    MemoryTierConfig config = new MemoryTierConfig();
    config.setMigrationTime(-1000);
    MemoryTierStorage<Entry> tier = new MemoryTierStorage<>(memory.proxy(), disk.proxy(), config);
    tier.add(new Entry(1));
    disk.blocked.set(true);
    AtomicReference<Throwable> migrationFailure = new AtomicReference<>();
    Thread scan = new Thread(() -> {
      try {
        tier.scan();
      } catch (Throwable e) {
        migrationFailure.set(e);
      }
    });
    scan.start();
    try {
      assertTrue(disk.entered.await(2, TimeUnit.SECONDS), "Migration did not start");
      long started = System.nanoTime();
      IOException timeout = assertThrows(IOException.class, tier::shutdown);
      long elapsed = System.nanoTime() - started;
      assertTrue(timeout.getMessage().contains("Timed out waiting for memory-tier migration"));
      assertTrue(elapsed >= TimeUnit.SECONDS.toNanos(4), "Shutdown returned before its timeout");
      assertFalse(memory.terminated.get(), "Memory tier closed after timeout");
      assertFalse(disk.terminated.get(), "Persistent tier closed after timeout");
    } finally {
      disk.release.countDown();
      scan.join(3000);
    }
    assertFalse(scan.isAlive(), "Migration did not finish after write was released");
    assertNull(migrationFailure.get());
    assertTrue(disk.records.containsKey(1L), "Completed write should remain persistent");
    assertFalse(memory.records.containsKey(1L), "Completed migration should remove memory copy");
  }

  @Test void shutdownPreventsFutureMigrationScans() throws Exception {
    Store<ObjectMonitor<Entry>> memory = new Store<>();
    Store<Entry> disk = new Store<>();
    MemoryTierConfig config = new MemoryTierConfig();
    config.setMigrationTime(-1000);
    MemoryTierStorage<Entry> tier = new MemoryTierStorage<>(memory.proxy(), disk.proxy(), config);
    tier.add(new Entry(1));
    tier.shutdown();
    tier.scan();
    assertTrue(disk.records.isEmpty());
  }
}
