package io.mapsmessaging.storage.impl.streams;

import java.io.IOException;
import java.io.RandomAccessFile;

public class RandomAccessFileObjectReader  extends ObjectReader {

  private final RandomAccessFile randomAccessFile;

  public RandomAccessFileObjectReader(RandomAccessFile randomAccessFile) {
    this.randomAccessFile = randomAccessFile;
  }

  @Override
  public byte readByte() throws IOException {
    return (byte) (0xff & randomAccessFile.read());
  }

  @Override
  public short readShort() throws IOException {
    return randomAccessFile.readShort();
  }

  @Override
  public int readInt() throws IOException {
    return randomAccessFile.readInt();
  }

  @Override
  public long readLong() throws IOException {
    return randomAccessFile.readLong();
  }

  @Override
  public char readChar() throws IOException {
    return randomAccessFile.readChar();
  }

  @Override
  public float readFloat() throws IOException {
    return randomAccessFile.readFloat();
  }

  @Override
  public double readDouble() throws IOException {
    return randomAccessFile.readDouble();
  }

  @Override
  public String readString() throws IOException {
    String result = null;
    int length = readInt();
    if (length > -1) {
      byte[] buffer = new byte[length];
      randomAccessFile.read(buffer);
      result = new String(buffer);
    }
    return result;
  }

  @Override
  protected byte[] readFromStream(int length) throws IOException {
    byte[] tmp = new byte[length];
    randomAccessFile.write(tmp);
    return tmp;
  }

  @Override
  protected long read(int size) throws IOException {
    byte[] tmp = new byte[size];
    randomAccessFile.read(tmp);
    return fromByteArray(tmp);
  }

  @Override
  public byte[] readByteArray() throws IOException {
    byte[] result = null;
    int length = readInt();
    if (length > -1) {
      result = new byte[length];
      randomAccessFile.read(result);
    }
    return result;
  }
}
