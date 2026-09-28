package io.mapsmessaging.storage.impl.file.partition;

import java.nio.ByteBuffer;
import java.nio.BufferOverflowException;
import java.nio.ReadOnlyBufferException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import static java.nio.file.StandardOpenOption.*;
import static org.junit.jupiter.api.Assertions.*;

class IndexPublicationTest {
  @Test
  void legacyReaderSeesIdenticalLayout() {
    ByteBuffer bytes = ByteBuffer.allocate(32); bytes.position(8);
    new IndexRecord(7, 3, 301394, 123456, 894).update(bytes);
    assertEquals(32, bytes.position()); bytes.position(8);
    assertEquals(301394, bytes.getLong());
    assertEquals(123456, bytes.getLong());
    assertEquals((3L << 32) | 894, bytes.getLong());
  }

  @Test
  void insufficientSpaceDoesNotPublishPosition() {
    ByteBuffer bytes = ByteBuffer.allocate(24); bytes.limit(23);
    assertThrows(BufferOverflowException.class, () -> new IndexRecord(7, 0, 99, 0, 10).update(bytes));
    assertEquals(0, bytes.getLong(0));
    assertEquals(0, bytes.position());
  }

  @Test
  void readOnlyFailureLeavesOriginalUntouched() {
    ByteBuffer bytes = ByteBuffer.allocate(24);
    assertThrows(ReadOnlyBufferException.class, () -> new IndexRecord(7, 0, 99, 0, 10).update(bytes.asReadOnlyBuffer()));
    assertEquals(0, bytes.getLong(0));
  }

  @Test
  void syncPublicationAndDeletionSurviveReopen() throws Exception {
    Path path = Files.createTempFile("index-publication", ".index");
    try {
      try (FileChannel file = FileChannel.open(path, READ, WRITE)) {
        new IndexRecord(7, 0, 99, 123, 10).update(file.map(FileChannel.MapMode.READ_WRITE, 0, 24), true);
      }
      try (FileChannel file = FileChannel.open(path, READ, WRITE)) {
        ByteBuffer data = ByteBuffer.allocate(24); file.read(data); data.flip();
        IndexRecord record = new IndexRecord(7, data);
        assertEquals(99, record.getPosition()); assertEquals(123, record.getExpiry()); assertEquals(10, record.getLength());
        new IndexRecord(7, 0, 0, 0, 10).update(file.map(FileChannel.MapMode.READ_WRITE, 0, 24), true);
      }
      ByteBuffer data = ByteBuffer.wrap(Files.readAllBytes(path));
      assertEquals(0, data.getLong()); assertEquals(0, data.getLong()); assertEquals(10, data.getLong());
    } finally { Files.delete(path); }
  }
}
