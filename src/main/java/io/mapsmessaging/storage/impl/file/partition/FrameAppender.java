package io.mapsmessaging.storage.impl.file.partition;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/** Completes a version-1 frame before its caller can publish an index entry. */
final class FrameAppender {
  interface Output {
    long size() throws IOException;
    void position(long position) throws IOException;
    long write(ByteBuffer[] buffers) throws IOException;
    void truncate(long size) throws IOException;
  }

  private final Output output;
  private IOException failure;

  FrameAppender(FileChannel channel) {
    this(new Output() {
      public long size() throws IOException { return channel.size(); }
      public void position(long position) throws IOException { channel.position(position); }
      public long write(ByteBuffer[] buffers) throws IOException { return channel.write(buffers); }
      public void truncate(long size) throws IOException { channel.truncate(size); }
    });
  }

  FrameAppender(Output output) { this.output = output; }

  synchronized long[] append(ByteBuffer[] payload) throws IOException {
    if (failure != null) {
      throw new IOException("Previous append rollback failed; reopen and validate the store", failure);
    }
    long payloadSize = 0;
    long headerSize = ((long) payload.length + 2) * Integer.BYTES;
    for (ByteBuffer buffer : payload) {
      payloadSize += buffer.remaining();
    }
    long expected = headerSize + payloadSize;
    if (expected > Integer.MAX_VALUE) {
      throw new IOException("Frame exceeds version-1 length limit");
    }
    ByteBuffer header = ByteBuffer.allocate((int) headerSize);
    // Preserve the historical field: payload bytes plus the buffer-count integer.
    header.putInt((int) payloadSize + Integer.BYTES).putInt(payload.length);
    ByteBuffer[] frame = new ByteBuffer[payload.length + 1];
    frame[0] = header;
    for (int i = 0; i < payload.length; i++) {
      header.putInt(payload[i].remaining());
      frame[i + 1] = payload[i].duplicate();
    }
    header.flip();
    long start = output.size();
    try {
      output.position(start);
      long written = 0;
      int stalled = 0;
      while (written < expected) {
        long count = output.write(frame);
        if (count < 0 || count > expected - written) {
          throw new IOException("Invalid append byte count: " + count);
        }
        if (count == 0) {
          if (++stalled >= 16) throw new IOException("Append made no progress");
        } else {
          stalled = 0;
          written += count;
        }
      }
      for (ByteBuffer buffer : frame) {
        if (buffer.hasRemaining()) throw new IOException("Incomplete append buffers");
      }
      if (output.size() != start + expected) throw new IOException("Unexpected file length after append");
      return new long[]{start, expected};
    } catch (IOException e) {
      try {
        output.truncate(start);
        output.position(start);
        if (output.size() != start) throw new IOException("Append rollback length mismatch");
      } catch (IOException rollback) {
        e.addSuppressed(rollback);
        failure = e;
      }
      throw e;
    }
  }
}
