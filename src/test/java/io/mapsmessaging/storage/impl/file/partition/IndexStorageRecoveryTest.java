/*
 * Copyright [ 2024 - 2026 ] MapsMessaging B.V.
 */
package io.mapsmessaging.storage.impl.file.partition;

import io.mapsmessaging.storage.impl.file.TaskQueue;
import io.mapsmessaging.storage.impl.file.config.PartitionStorageConfig;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static io.mapsmessaging.storage.impl.file.partition.RecoveryTestData.*;

class IndexStorageRecoveryTest {
  @TempDir Path directory;

  @Test
  void rejectedPublicationBlocksFurtherAppendsUntilReopen() throws Exception {
    IndexStorage<Entry> storage = open("publication");
    try {
      storage.add(new Entry(0, 0));
      IOException failure = assertThrows(IOException.class, () -> storage.add(new Entry(4, 0)));
      long failedLength = storage.length();
      IOException blocked = assertThrows(IOException.class, () -> storage.add(new Entry(1, 0)));
      assertSame(failure, blocked.getCause());
      assertEquals(failedLength, storage.length());
      assertEquals(List.of(0L), storage.getKeys());
    } finally { storage.close(); }

    IndexStorage<Entry> reopened = open("publication");
    try {
      reopened.add(new Entry(1, 0));
      assertEquals(List.of(0L, 1L), reopened.getKeys());
      assertEquals(new Entry(0, 0), reopened.get(0).getObject());
    } finally { reopened.close(); }
  }

  @Test
  void pausedAddResumesAndPreservesExistingRecords() throws Exception {
    IndexStorage<Entry> storage = open("paused-add");
    try {
      storage.add(new Entry(0, 0));
      storage.pause();
      assertEquals(0, storage.emptySpace());
      storage.add(new Entry(1, 0));
      assertEquals(List.of(0L, 1L), storage.getKeys());
      assertEquals(new Entry(0, 0), storage.get(0).getObject());
    } finally { storage.close(); }
  }

  @Test
  void bulkRemovalResumesAndCountsOnlyExistingKeys() throws Exception {
    IndexStorage<Entry> storage = open("remove");
    try {
      storage.add(new Entry(0, 0));
      storage.add(new Entry(1, 0));
      assertEquals(0, storage.removeAll(List.of()));
      storage.pause();
      assertEquals(1, storage.removeAll(List.of(0L, 0L, 9L)));
      assertEquals(List.of(1L), storage.getKeys());
      assertNull(storage.get(-1));
      assertNull(storage.get(9));
    } finally { storage.close(); }
  }

  @Test
  void uncleanReopenDropsTruncatedRecordAndRetainsEarlierRecord() throws Exception {
    IndexStorage<Entry> storage = open("recovery");
    try {
      storage.add(new Entry(0, 0));
      storage.add(new Entry(1, 0));
    } finally { storage.close(); }
    Path data = directory.resolve("recovery_index_data");
    try (var channel = java.nio.channels.FileChannel.open(data, java.nio.file.StandardOpenOption.WRITE)) {
      channel.truncate(Files.size(data) - 1);
      channel.write(java.nio.ByteBuffer.allocate(8).putLong(DataStorageImpl.OPEN_STATE).flip(), 0);
    }
    storage = open("recovery");
    try {
      assertEquals(List.of(0L), storage.getKeys());
      assertFalse(storage.contains(1));
      assertEquals(new Entry(0, 0), storage.get(0).getObject());
    } finally { storage.close(); }
  }

  private IndexStorage<Entry> open(String name) throws IOException {
    PartitionStorageConfig config = new PartitionStorageConfig();
    config.fromMap(Map.of("ItemCount", "4", "MaxPartitionSize", "4096"));
    config.setStorableFactory(FACTORY);
    return new IndexStorage<>(config, directory.resolve(name).toString(), 0, new TaskQueue());
  }
}
