package io.mapsmessaging.storage.impl;

import java.io.IOException;
import java.io.InputStream;

public class StreamObjectReader extends ObjectReader {

  private final InputStream inputStream;

  public StreamObjectReader(InputStream inputStream) {
    this.inputStream = inputStream;
  }

  @Override
  public byte readByte() throws IOException {
    return (byte) (0xff & inputStream.read());
  }

  @Override
  public char readChar() throws IOException {
    return (char) readShort();
  }

  @Override

  protected long read(int size) throws IOException {
    return fromByteArray(readFromStream(size));
  }

  protected byte[] readFromStream(int length) throws IOException {
    byte[] result = null;
    if(length > -1) {
      result = new byte[length];
      int read = 0;
      while (read < length) {
        int t = inputStream.read(result, read, length - read);
        if (t < 0) {
          throw new IOException("End of stream encountered");
        }
        read += t;
      }
    }
    return result;
  }
}
