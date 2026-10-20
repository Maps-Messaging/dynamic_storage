/*
 * Copyright [ 2024 - 2026 ] MapsMessaging B.V.
 */
package io.mapsmessaging.storage.impl.file.partition.deferred;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

class DeferredRecordReliabilityTest {

  @Test
  void archiveTimestampUsesUtcClockBasis() {
    LocalDateTime before = LocalDateTime.now(ZoneOffset.UTC);
    TestDeferredRecord record = new TestDeferredRecord("SHA-256", "hash", 42);
    LocalDateTime after = LocalDateTime.now(ZoneOffset.UTC);

    assertNotNull(record.getArchivedDate());
    assertFalse(record.getArchivedDate().isBefore(before));
    assertFalse(record.getArchivedDate().isAfter(after));
  }

  private static final class TestDeferredRecord extends DeferredRecord {

    private TestDeferredRecord(String digestName, String deferredHash, long length) {
      super(digestName, deferredHash, length);
    }

    @Override
    public void read(String filename) throws IOException {
      // not required for constructor behaviour
    }
  }
}
