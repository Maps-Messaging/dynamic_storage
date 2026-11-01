package io.mapsmessaging.storage.impl.file.partition;

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.StorableFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.junit.jupiter.api.Assertions.*;

class DataStorageLazyValidationTest {

  private record Entry(long key) implements Storable {
    @Override public long getKey() { return key; }
    @Override public long getExpiry() { return 0; }
  }

  private static final StorableFactory<Entry> FACTORY = new StorableFactory<>() {
    @Override public ByteBuffer[] pack(Entry entry) {
      return new ByteBuffer[] {ByteBuffer.allocate(Long.BYTES).putLong(0, entry.key())};
    }

    @Override public Entry unpack(ByteBuffer[] buffers) {
      return new Entry(buffers[0].getLong());
    }
  };

  @Test
  void validFrameLoadsWithMatchingKey(@TempDir Path temp) throws Exception {
    try (DataStorageImpl<Entry> store = new DataStorageImpl<>(temp.resolve("data").toString(), FACTORY, false, 1024)) {
      IndexRecord record = store.add(new Entry(41));
      assertTrue(store.isValid(record));
      assertEquals(41, store.get(record).getKey());
    }
  }

  @Test
  void mismatchedDecodedKeyIsRejected(@TempDir Path temp) throws Exception {
    Path file = temp.resolve("data");
    try (DataStorageImpl<Entry> store = new DataStorageImpl<>(file.toString(), FACTORY, false, 1024)) {
      IndexRecord record = store.add(new Entry(41));
      // One payload buffer: 8-byte frame header + 4-byte length table.
      try (FileChannel channel = FileChannel.open(file, READ, WRITE)) {
        channel.write(ByteBuffer.allocate(Long.BYTES).putLong(0, 42), record.getPosition() + 12);
      }
      assertTrue(store.isValid(record), "Structural validation does not deserialize the payload");
      assertThrows(IOException.class, () -> store.get(record));
    }
  }

  @Test
  void inconsistentFrameLengthIsRejectedBeforeDeserialisation(@TempDir Path temp) throws Exception {
    Path file = temp.resolve("data");
    try (DataStorageImpl<Entry> store = new DataStorageImpl<>(file.toString(), FACTORY, false, 1024)) {
      IndexRecord record = store.add(new Entry(41));
      try (FileChannel channel = FileChannel.open(file, READ, WRITE)) {
        channel.write(ByteBuffer.allocate(Integer.BYTES).putInt(0, 99), record.getPosition());
      }
      assertFalse(store.isValid(record));
      assertThrows(IOException.class, () -> store.get(record));
    }
  }

  @Test
  void truncatedFrameIsRejected(@TempDir Path temp) throws Exception {
    Path file = temp.resolve("data");
    try (DataStorageImpl<Entry> store = new DataStorageImpl<>(file.toString(), FACTORY, false, 1024)) {
      IndexRecord record = store.add(new Entry(41));
      try (FileChannel channel = FileChannel.open(file, WRITE)) {
        channel.truncate(record.getPosition() + record.getLength() - 1);
      }
      assertFalse(store.isValid(record));
      assertThrows(IOException.class, () -> store.get(record));
    }
  }
}
