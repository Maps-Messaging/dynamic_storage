package io.mapsmessaging.storage.impl.tier.memory;

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class MemoryTierFailureTest {
  private record Entry(long key) implements Storable {
    @Override public long getKey() { return key; }
    @Override public long getExpiry() { return 0; }
  }

  private static final class Store<T extends Storable> {
    final Map<Long, T> records = new LinkedHashMap<>();
    final AtomicBoolean fail = new AtomicBoolean();

    @SuppressWarnings("unchecked")
    Storage<T> storage() {
      return (Storage<T>) Proxy.newProxyInstance(Storage.class.getClassLoader(),
          new Class<?>[]{Storage.class}, (proxy, method, args) -> switch (method.getName()) {
            case "add" -> {
              if (fail.get()) throw new IOException("disk full");
              T value = (T) args[0];
              records.put(value.getKey(), value);
              yield null;
            }
            case "get" -> records.get((Long) args[0]);
            case "remove" -> records.remove((Long) args[0]) != null;
            case "getKeys" -> List.copyOf(records.keySet());
            case "contains" -> records.containsKey((Long) args[0]);
            case "size" -> (long) records.size();
            case "close", "delete", "shutdown", "setExecutor" -> null;
            default -> throw new UnsupportedOperationException(method.getName());
          });
    }
  }

  @Test
  void capacityEvictionRetriesFailedWriteWithoutLosingRecord() throws Exception {
    Store<ObjectMonitor<Entry>> memory = new Store<>();
    Store<Entry> disk = new Store<>();
    MemoryTierConfig config = new MemoryTierConfig();
    config.setMaximumCount(1);
    MemoryTierStorage<Entry> tier = new MemoryTierStorage<>(memory.storage(), disk.storage(), config);
    tier.add(new Entry(1));
    disk.fail.set(true);
    assertThrows(IOException.class, () -> tier.add(new Entry(2)));
    assertTrue(memory.records.containsKey(1L));
    assertFalse(disk.records.containsKey(1L));
    disk.fail.set(false);
    tier.add(new Entry(3));
    assertTrue(disk.records.containsKey(1L), "Failed eviction must be retried");
  }

  @Test
  void timedMigrationNotifiesFailureAndRetainsMemory() {
    Store<ObjectMonitor<Entry>> memory = new Store<>();
    Store<Entry> disk = new Store<>();
    disk.fail.set(true);
    MemoryTierConfig config = new MemoryTierConfig();
    config.setMigrationTime(-1000);
    MemoryTierStorage<Entry> tier = new MemoryTierStorage<>(memory.storage(), disk.storage(), config);
    AtomicInteger notifications = new AtomicInteger();
    tier.setMigrationFailureHandler((key, failure) -> {
      assertEquals(1L, key);
      assertEquals("disk full", failure.getMessage());
      notifications.incrementAndGet();
    });
    assertDoesNotThrow(() -> tier.add(new Entry(1)));
    assertDoesNotThrow(tier::scan);
    assertEquals(1, notifications.get());
    assertTrue(memory.records.containsKey(1L));
    assertTrue(disk.records.isEmpty());
    disk.fail.set(false);
    tier.scan();
    assertFalse(memory.records.containsKey(1L));
    assertTrue(disk.records.containsKey(1L));
  }
}
