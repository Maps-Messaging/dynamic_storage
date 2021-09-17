package io.mapsmessaging.storage.impl;

import java.io.IOException;
import java.nio.ByteBuffer;

public class BufferObjectReader extends ObjectReader {

  private final ByteBuffer buffer;

  public BufferObjectReader(ByteBuffer buffer) {
    this.buffer = buffer;
  }

  @Override
  public byte readByte() {
    return buffer.get();
  }

  @Override
  public char readChar() {
    return buffer.getChar();
  }

  @Override
  public String readString() throws IOException {
    String result = null;
    int length = readInt();
    if (length > -1) {
      byte[] tmp = new byte[length];
      buffer.get(tmp);
      result = new String(tmp);
    }
    return result;
  }

  @Override
  public byte[] readByteArray() throws IOException {
    int length = readInt();
    return readFromStream(length);
  }

  protected long read(int size) throws IOException {
    byte[] tmp = new byte[size];
    if (buffer.limit() - buffer.position() < size) {
      throw new IOException("End Of buffer reached");
    }
    buffer.get(tmp);
    return fromByteArray(tmp);
  }

  @Override
  public byte[] readFromStream(int length) {
    byte[] result = null;
    if (length > -1) {
      result = new byte[length];
      buffer.get(result);
    }
    return result;
  }

}
