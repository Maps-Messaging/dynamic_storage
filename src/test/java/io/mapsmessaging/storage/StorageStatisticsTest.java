/*
 * Copyright [ 2024 - 2026 ] MapsMessaging B.V.
 */
package io.mapsmessaging.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class StorageStatisticsTest {

  @Test
  void calculatesIopsLatenciesAndFormatsStatistics() {
    StorageStatistics statistics =
        new StorageStatistics(4, 2, 1, 100, 200, 40, 30, 300, 25, 3);

    assertEquals(6, statistics.getIops());
    assertEquals(10, statistics.getReadLatency());
    assertEquals(15, statistics.getWriteLatency());

    String text = statistics.toString();
    assertTrue(text.contains("Reads:4"));
    assertTrue(text.contains("Writes:2"));
    assertTrue(text.contains("IOPS:6"));
    assertTrue(text.contains("File Count:3"));
  }

  @Test
  void zeroActivityProducesZeroLatencies() {
    StorageStatistics statistics =
        new StorageStatistics(0, 0, 0, 0, 0, 100, 100, 0, 0, 0);

    assertEquals(0, statistics.getReadLatency());
    assertEquals(0, statistics.getWriteLatency());
  }
}
