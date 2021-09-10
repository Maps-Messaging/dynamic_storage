package io.mapsmessaging.storage.impl.streams;

import java.io.IOException;
import java.io.OutputStream;

public class StreamObjectWriter extends ObjectWriter {

  private final OutputStream outputStream;

  public StreamObjectWriter(OutputStream outputStream) {
    this.outputStream = outputStream;
  }

  // <editor-fold desc="Native Integer based value support">
  @Override
  public void write(byte val) throws IOException {
    outputStream.write(val);
  }

  @Override
  public void write(char val) throws IOException {
    outputStream.write((val>>8)&0xff);
    outputStream.write(val&0xff);
  }
  // </editor-fold>

  @Override
  public void write(byte[] val) throws IOException {
    if (val == null) {
      write(-1);
      return;
    }
    write(val.length);
    if (val.length > 0) {
      outputStream.write(val);
    }
  }

  protected void write(long val, int size) throws IOException {
    outputStream.write(toByteArray(val, size));
  }
}