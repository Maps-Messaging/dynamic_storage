/*
 * Copyright [ 2024 - 2026 ] MapsMessaging B.V.
 */
package io.mapsmessaging.storage.impl.file.partition;

import io.mapsmessaging.storage.StorableFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import static java.nio.file.StandardOpenOption.*;
import static org.junit.jupiter.api.Assertions.*;
import static io.mapsmessaging.storage.impl.file.partition.RecoveryTestData.*;

class DataStorageRecoveryTest {
  @TempDir Path directory;

  @ParameterizedTest
  @CsvSource({"8,0", "16,0"})
  void rejectsCorruptFileHeader(int offset, long value) throws Exception {
    Path file = directory.resolve("header.data");
    try (DataStorageImpl<Entry> storage = open(file)) { storage.add(new Entry(1, 0)); }
    try (FileChannel channel = FileChannel.open(file, WRITE)) {
      channel.write(ByteBuffer.allocate(8).putLong(value).flip(), offset);
    }
    assertThrows(IOException.class, () -> open(file));
  }

  @Test
  void rejectsTruncatedFileHeader() throws Exception {
    Path file = directory.resolve("short.data");
    Files.write(file, new byte[] {1});
    assertThrows(IOException.class, () -> open(file));
  }

  @ParameterizedTest
  @CsvSource({"0,0", "4,0", "4,-1", "4,1025", "8,-1", "8,1000", "8,20"})
  void rejectsMalformedRecordMetadata(int offset, int value) throws Exception {
    Path file = directory.resolve("metadata.data");
    try (DataStorageImpl<Entry> storage = open(file)) {
      IndexRecord record = storage.add(new Entry(1, 0));
      assertTrue(storage.isValid(record));
      try (FileChannel channel = FileChannel.open(file, WRITE)) {
        channel.write(ByteBuffer.allocate(4).putInt(value).flip(), record.getPosition() + offset);
      }
      assertFalse(storage.isValid(record));
    }
  }

  @Test
  void rejectsInvalidRecordBounds() throws Exception {
    try (DataStorageImpl<Entry> storage = open(directory.resolve("bounds.data"))) {
      storage.add(new Entry(1, 0));
      assertFalse(storage.isValid(null));
      assertFalse(storage.isValid(new IndexRecord(1, 0, 0, 0, 28)));
      assertFalse(storage.isValid(new IndexRecord(1, 0, 24, 0, 0)));
      assertFalse(storage.isValid(new IndexRecord(1, 0, 1, 0, 28)));
      assertFalse(storage.isValid(new IndexRecord(1, 0, Long.MAX_VALUE, 0, 28)));
      assertFalse(storage.isValid(new IndexRecord(1, 0, 24, 0, 100)));
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(ints = {4, 10, 20})
  void truncatedRecordFailsWithoutReturningPartialObject(int remaining) throws Exception {
    Path file = directory.resolve("truncated.data");
    try (DataStorageImpl<Entry> storage = open(file)) {
      IndexRecord record = storage.add(new Entry(1, 0));
      try (FileChannel channel = FileChannel.open(file, WRITE)) {
        channel.truncate(record.getPosition() + remaining);
      }
      assertFalse(storage.isValid(record));
      assertThrows(IOException.class, () -> storage.get(record));
    }
  }

  @Test
  void unpackFailureMarksRecordInvalid() throws Exception {
    Path file = directory.resolve("unpack.data");
    StorableFactory<Entry> failing = new StorableFactory<>() {
      public ByteBuffer[] pack(Entry entry) throws IOException { return FACTORY.pack(entry); }
      public Entry unpack(ByteBuffer[] buffers) throws IOException { throw new IOException("Invalid payload"); }
    };
    try (DataStorageImpl<Entry> storage = new DataStorageImpl<>(file.toString(), failing, false, 4096)) {
      assertFalse(storage.isValid(storage.add(new Entry(1, 0))));
    }
  }

  private DataStorageImpl<Entry> open(Path file) throws IOException {
    return new DataStorageImpl<>(file.toString(), FACTORY, false, 4096);
  }
}
