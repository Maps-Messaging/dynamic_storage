/*
 * Copyright [ 2024 - 2026 ] MapsMessaging B.V.
 */
package io.mapsmessaging.storage.impl.file.partition;

import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static java.nio.file.StandardOpenOption.*;
import static org.junit.jupiter.api.Assertions.*;

class IndexManagerRecoveryTest {
  @TempDir Path directory;

  @Test
  void remapPreservesRecordsAndAllowsFurtherPublication() throws Exception {
    try (FileChannel channel = FileChannel.open(directory.resolve("remap"), CREATE, READ, WRITE);
         IndexManager manager = new IndexManager(10, 4, channel)) {
      manager.loadMap(false);
      assertTrue(manager.add(10, record(10)));
      manager.pause();
      manager.pause();
      manager.resume(channel);
      manager.resume(channel);
      assertEquals(24, manager.get(10).getPosition());
      assertTrue(manager.add(11, record(11)));
      assertEquals(List.of(10L, 11L), manager.keySet());
    }
  }

  @Test
  void rejectsKeysOutsideConfiguredAndReducedRange() throws Exception {
    try (FileChannel channel = FileChannel.open(directory.resolve("bounds"), CREATE, READ, WRITE);
         IndexManager manager = new IndexManager(10, 4, channel)) {
      manager.loadMap(false);
      manager.setEnd(11);
      for (long key : new long[] {9, 12, 14}) {
        assertFalse(manager.add(key, record(key)));
        assertNull(manager.get(key));
        assertFalse(manager.contains(key));
        assertFalse(manager.delete(key));
      }
      manager.close();
      assertFalse(manager.add(10, record(10)));
      assertNull(manager.get(10));
      assertFalse(manager.delete(10));
    }
  }

  @Test
  void rebuildRetainsExpiryAndIteratorRemovalDeletesRecord() throws Exception {
    try (FileChannel channel = FileChannel.open(directory.resolve("rebuild"), CREATE, READ, WRITE);
         IndexManager manager = new IndexManager(10, 1, channel)) {
      manager.loadMap(false);
      assertTrue(manager.add(10, new IndexRecord(10, 0, 24, Long.MAX_VALUE, 28)));
      manager.rebuild();
      assertEquals(1, manager.size());
      assertEquals(List.of(10L), manager.getExpiryIndex());
      ArrayDeque<Long> expired = new ArrayDeque<>();
      manager.scanForExpired(expired);
      assertTrue(expired.isEmpty());
      Iterator<IndexRecord> iterator = manager.getIterator();
      assertEquals(10, iterator.next().getKey());
      iterator.remove();
      assertFalse(manager.contains(10));
      assertEquals(0, manager.size());
      assertFalse(iterator.hasNext());
      assertThrows(NoSuchElementException.class, iterator::next);
    }
  }

  private static IndexRecord record(long key) {
    return new IndexRecord(key, 0, 24, 0, 28);
  }
}
