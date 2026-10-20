/*
 * Copyright [ 2024 - 2026 ] MapsMessaging B.V.
 */
package io.mapsmessaging.storage.impl.file.partition;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.*;

class FrameAppenderRecoveryTest {
  @ParameterizedTest
  @ValueSource(longs = {-1, 100})
  void invalidWriteCountRollsBackAndAllowsRetry(long count) throws Exception {
    FrameAppenderChecks.MemoryOutput output = new FrameAppenderChecks.MemoryOutput() {
      boolean first = true;
      public long write(ByteBuffer[] buffers) throws IOException {
        if (first) { first = false; return count; }
        return super.write(buffers);
      }
    };
    FrameAppender appender = new FrameAppender(output);
    assertThrows(IOException.class, () -> appender.append(payload()));
    assertEquals(24, output.size);
    assertEquals(42, output.data[0]);
    assertArrayEquals(new long[] {24, 15}, appender.append(payload()));
  }

  @Test
  void reportedCompletionWithRemainingBuffersIsRejected() {
    FrameAppenderChecks.MemoryOutput output = new FrameAppenderChecks.MemoryOutput() {
      public long write(ByteBuffer[] buffers) { return 15; }
    };
    assertThrows(IOException.class, () -> new FrameAppender(output).append(payload()));
    assertEquals(24, output.size);
    assertEquals(42, output.data[0]);
  }

  @Test
  void unexpectedSizeAfterWriteRollsBack() {
    FrameAppenderChecks.MemoryOutput output = new FrameAppenderChecks.MemoryOutput() {
      public long write(ByteBuffer[] buffers) throws IOException {
        long written = super.write(buffers);
        size++;
        return written;
      }
    };
    assertThrows(IOException.class, () -> new FrameAppender(output).append(payload()));
    assertEquals(24, output.size);
    assertEquals(42, output.data[0]);
  }

  @Test
  void rollbackLengthMismatchPoisonsAppender() throws Exception {
    FrameAppenderChecks.MemoryOutput output = new FrameAppenderChecks.MemoryOutput() {
      public void truncate(long size) { /* Fault: disk length remains unchanged. */ }
    };
    output.budget = 5;
    FrameAppender appender = new FrameAppender(output);
    IOException original = assertThrows(IOException.class, () -> appender.append(payload()));
    assertEquals(1, original.getSuppressed().length);
    int calls = output.calls;
    output.budget = 1000;
    IOException next = assertThrows(IOException.class, () -> appender.append(payload()));
    assertSame(original, next.getCause());
    assertEquals(calls, output.calls);
    assertEquals(42, output.data[0]);
  }

  private static ByteBuffer[] payload() {
    return new ByteBuffer[] {ByteBuffer.wrap(new byte[] {1, 2, 3})};
  }
}
