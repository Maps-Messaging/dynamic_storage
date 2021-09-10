package io.mapsmessaging.storage.impl.streams;

import java.io.DataOutput;
import java.io.IOException;

public class DataObjectWriter extends ObjectWriter {

  private final DataOutput dataOutput;

  public DataObjectWriter(DataOutput dataOutput) {
    this.dataOutput = dataOutput;
  }

  // <editor-fold desc="Native Integer based value support">
  @Override
  public void write(byte val) throws IOException {
    dataOutput.write(val);
  }

  @Override
  public void write(char val) throws IOException {
    dataOutput.writeChar(val);
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
      dataOutput.write(val);
    }
  }

  protected void write(long val, int size) throws IOException {
    dataOutput.write(toByteArray(val, size));
  }
}
