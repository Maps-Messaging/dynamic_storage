package io.mapsmessaging.storage.impl.file.partition;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.channels.FileChannel;
import static java.nio.file.StandardOpenOption.*;

/** Dependency-free fault injection checks, also run by FrameAppenderTest. */
public class FrameAppenderChecks {
  public static void main(String[] args) throws Exception {
    shortWritesAndLegacyFrame();
    diskFull(0);
    diskFull(7);
    noProgress();
    rollbackFailure();
    realFileAppend();
    System.out.println("6 append checks passed");
  }

  static void check(boolean condition) { if (!condition) throw new AssertionError(); }
  static IOException fails(FrameAppender appender) throws Exception {
    try { appender.append(new ByteBuffer[]{ByteBuffer.wrap(new byte[]{1, 2, 3})}); }
    catch (IOException expected) { return expected; }
    throw new AssertionError("Expected IOException");
  }

  static void shortWritesAndLegacyFrame() throws Exception {
    MemoryOutput out = new MemoryOutput(); out.chunk = 2;
    ByteBuffer payload = ByteBuffer.wrap(new byte[]{9, 1, 2, 3}); payload.position(1);
    long[] result = new FrameAppender(out).append(new ByteBuffer[]{payload});
    check(result[0] == 24 && result[1] == 15 && payload.position() == 1);
    ByteBuffer legacy = ByteBuffer.wrap(out.data); legacy.position(24);
    check(legacy.getInt() == 7 && legacy.getInt() == 1 && legacy.getInt() == 3);
    check(legacy.get() == 1 && legacy.get() == 2 && legacy.get() == 3);
  }

  static void diskFull(int partial) throws Exception {
    MemoryOutput out = new MemoryOutput(); out.budget = partial;
    FrameAppender appender = new FrameAppender(out);
    check(fails(appender).getMessage().contains("No space"));
    check(out.size == 24);
    out.budget = 1000;
    check(appender.append(new ByteBuffer[]{ByteBuffer.wrap(new byte[]{4})})[0] == 24);
    check(out.data[0] == 42); // Existing file prefix survives rollback.
  }

  static void noProgress() throws Exception {
    MemoryOutput out = new MemoryOutput(); out.chunk = 0;
    fails(new FrameAppender(out)); check(out.calls == 16 && out.size == 24);
  }

  static void rollbackFailure() throws Exception {
    MemoryOutput out = new MemoryOutput(); out.budget = 5; out.failTruncate = true;
    FrameAppender appender = new FrameAppender(out);
    check(fails(appender).getSuppressed().length == 1);
    int calls = out.calls; out.budget = 1000;
    check(fails(appender).getMessage().contains("rollback failed"));
    check(out.calls == calls);
  }

  static void realFileAppend() throws Exception {
    Path path = Files.createTempFile("append-check", ".data");
    try (FileChannel channel = FileChannel.open(path, READ, WRITE)) {
      channel.write(ByteBuffer.allocate(24));
      FrameAppender appender = new FrameAppender(channel);
      long[] first = appender.append(new ByteBuffer[]{ByteBuffer.wrap(new byte[]{1, 2})});
      long[] second = appender.append(new ByteBuffer[]{ByteBuffer.wrap(new byte[]{3})});
      check(first[0] == 24 && first[1] == 14 && second[0] == 38 && channel.size() == 51);
    } finally { Files.delete(path); }
  }

  static class MemoryOutput implements FrameAppender.Output {
    byte[] data = new byte[1024];
    int size = 24, position = 24, chunk = 1000, budget = 1000, calls;
    boolean failTruncate;
    MemoryOutput() { data[0] = 42; }
    public long size() { return size; }
    public void position(long value) { position = (int) value; }
    public void truncate(long value) throws IOException {
      if (failTruncate) throw new IOException("Rollback I/O failure");
      size = (int) value;
    }
    public long write(ByteBuffer[] buffers) throws IOException {
      calls++;
      if (budget == 0) throw new IOException("No space left on device");
      int limit = Math.min(chunk, budget), written = 0;
      for (ByteBuffer buffer : buffers) {
        while (buffer.hasRemaining() && written < limit) {
          data[position++] = buffer.get(); written++; budget--;
        }
      }
      size = Math.max(size, position);
      return written;
    }
  }
}
