package io.mapsmessaging.storage.streams;

import java.io.IOException;
import java.nio.ByteBuffer;

public class BufferObjectWriter extends ObjectWriter {

  private final ByteBuffer buffer;

  public BufferObjectWriter(ByteBuffer buffer) {
    this.buffer = buffer;
  }

  @Override
  public void write(byte val) throws IOException {
    buffer.put(val);
  }

  @Override
  public void write(char val) throws IOException {
    buffer.putChar(val);
  }

  @Override
  public void write(byte[] val) throws IOException {
    if (val == null) {
      write(-1);
    } else {
      write(val.length);
      if (val.length > 0) {
        buffer.put(val);
      }
    }
  }

  protected void write(long val, int size) {
    buffer.put(toByteArray(val, size));
  }

}