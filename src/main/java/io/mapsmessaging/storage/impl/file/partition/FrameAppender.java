package io.mapsmessaging.storage.impl.file.partition;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/** Completes a version-1 frame before its caller can publish an index entry. */
final class FrameAppender {

  private static final int MAX_STALLED_WRITES = 16;

  interface Output {
    long size() throws IOException;

    void position(long position) throws IOException;

    long write(ByteBuffer[] buffers) throws IOException;

    void truncate(long size) throws IOException;
  }

  private final Output output;
  private IOException failure;

  FrameAppender(FileChannel channel) {
    this(
        new Output() {
          public long size() throws IOException {
            return channel.size();
          }

          public void position(long position) throws IOException {
            channel.position(position);
          }

          public long write(ByteBuffer[] buffers) throws IOException {
            return channel.write(buffers);
          }

          public void truncate(long size) throws IOException {
            channel.truncate(size);
          }
        });
  }

  FrameAppender(Output output) {
    this.output = output;
  }

  synchronized long[] append(ByteBuffer[] payload) throws IOException {
    ensureAppenderUsable();

    Frame frame = buildFrame(payload);
    long start = output.size();

    try {
      writeFrame(start, frame);
      validateCompletedFrame(start, frame);
      return new long[] {start, frame.expectedLength()};
    } catch (IOException e) {
      rollback(start, e);
      throw e;
    }
  }

  private void ensureAppenderUsable() throws IOException {
    if (failure != null) {
      throw new IOException(
          "Previous append rollback failed; reopen and validate the store", failure);
    }
  }

  private Frame buildFrame(ByteBuffer[] payload) throws IOException {
    long payloadSize = payloadSize(payload);
    long headerSize = ((long) payload.length + 2) * Integer.BYTES;
    long expected = headerSize + payloadSize;
    if (expected > Integer.MAX_VALUE) {
      throw new IOException("Frame exceeds version-1 length limit");
    }

    ByteBuffer header = ByteBuffer.allocate((int) headerSize);
    header.putInt((int) payloadSize + Integer.BYTES).putInt(payload.length);

    ByteBuffer[] buffers = new ByteBuffer[payload.length + 1];
    buffers[0] = header;
    for (int i = 0; i < payload.length; i++) {
      header.putInt(payload[i].remaining());
      buffers[i + 1] = payload[i].duplicate();
    }
    header.flip();

    return new Frame(buffers, expected);
  }

  private long payloadSize(ByteBuffer[] payload) {
    long size = 0;
    for (ByteBuffer buffer : payload) {
      size += buffer.remaining();
    }
    return size;
  }

  private void writeFrame(long start, Frame frame) throws IOException {
    output.position(start);

    long written = 0;
    int stalled = 0;
    while (written < frame.expectedLength()) {
      long count = output.write(frame.buffers());
      validateWriteCount(count, frame.expectedLength() - written);

      if (count == 0) {
        stalled = incrementStalledWrites(stalled);
      } else {
        stalled = 0;
        written += count;
      }
    }
  }

  private void validateWriteCount(long count, long remaining) throws IOException {
    if (count < 0 || count > remaining) {
      throw new IOException("Invalid append byte count: " + count);
    }
  }

  private int incrementStalledWrites(int stalled) throws IOException {
    int next = stalled + 1;
    if (next >= MAX_STALLED_WRITES) {
      throw new IOException("Append made no progress");
    }
    return next;
  }

  private void validateCompletedFrame(long start, Frame frame) throws IOException {
    for (ByteBuffer buffer : frame.buffers()) {
      if (buffer.hasRemaining()) {
        throw new IOException("Incomplete append buffers");
      }
    }

    if (output.size() != start + frame.expectedLength()) {
      throw new IOException("Unexpected file length after append");
    }
  }

  private void rollback(long start, IOException appendFailure) {
    try {
      output.truncate(start);
      output.position(start);
      if (output.size() != start) {
        throw new IOException("Append rollback length mismatch");
      }
    } catch (IOException rollbackFailure) {
      appendFailure.addSuppressed(rollbackFailure);
      failure = appendFailure;
    }
  }

  private static final class Frame {
    private final ByteBuffer[] buffers;
    private final long expectedLength;

    private Frame(ByteBuffer[] buffers, long expectedLength) {
      this.buffers = buffers;
      this.expectedLength = expectedLength;
    }

    private ByteBuffer[] buffers() {
      return buffers;
    }

    private long expectedLength() {
      return expectedLength;
    }
  }
}
