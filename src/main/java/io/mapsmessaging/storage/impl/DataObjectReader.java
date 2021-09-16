package io.mapsmessaging.storage.impl;

import java.io.DataInput;
import java.io.IOException;

public class DataObjectReader extends ObjectReader {

  private final DataInput dataInput;

  public DataObjectReader(DataInput dataInput) {
    this.dataInput = dataInput;
  }

  @Override
  public byte readByte() throws IOException {
    return (byte) (0xff & dataInput.readByte());
  }

  @Override
  public char readChar() throws IOException {
    return dataInput.readChar();
  }

  protected long read(int size) throws IOException {
    return fromByteArray(readFromStream(size));
  }

  protected byte[] readFromStream(int length) throws IOException {
    byte[] result = null;
    if (length > -1) {
      result = new byte[length];
      dataInput.readFully(result);
    }
    return result;
  }
}
