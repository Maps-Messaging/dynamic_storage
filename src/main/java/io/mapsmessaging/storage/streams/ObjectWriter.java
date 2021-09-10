package io.mapsmessaging.storage.streams;

import java.io.IOException;

public abstract class ObjectWriter {

  public void write(short val) throws IOException {
    write(val, 2);
  }

  public void write(int val) throws IOException {
    write(val, 4);
  }

  public void write(long val) throws IOException {
    write(val, 8);
  }

  public void write(float val) throws IOException {
    write(Float.floatToIntBits(val), 4);
  }

  public void write(double val) throws IOException {
    write(Double.doubleToRawLongBits(val), 8);
  }

  public void write(String val) throws IOException {
    if (val == null) {
      write(-1);
      return;
    }
    write(val.getBytes());
  }

  // <editor-fold desc="Native array type support">
  public void write(char[] val) throws IOException {
    if (val == null) {
      write(-1);
      return;
    }
    write(val.length);
    if (val.length > 0) {
      for (char entry : val) {
        write(entry);
      }
    }
  }

  public void write(short[] val) throws IOException {
    if (val == null) {
      write(-1);
      return;
    }
    write(val.length);
    if (val.length > 0) {
      for (short entry : val) {
        write(entry);
      }
    }
  }

  public void write(int[] val) throws IOException {
    if (val == null) {
      write(-1);
      return;
    }
    write(val.length);
    if (val.length > 0) {
      for (int entry : val) {
        write(entry);
      }
    }
  }

  public void write(long[] val) throws IOException {
    if (val == null) {
      write(-1);
      return;
    }
    write(val.length);
    if (val.length > 0) {
      for (long entry : val) {
        write(entry);
      }
    }
  }

  public void write(float[] val) throws IOException {
    if (val == null) {
      write(-1);
      return;
    }
    write(val.length);
    if (val.length > 0) {
      for (float entry : val) {
        write(entry);
      }
    }
  }

  public void write(double[] val) throws IOException {
    if (val == null) {
      write(-1);
      return;
    }
    write(val.length);
    if (val.length > 0) {
      for (double entry : val) {
        write(entry);
      }
    }
  }

  public void write(String[] val) throws IOException {
    if (val == null) {
      write(-1);
      return;
    }
    write(val.length);
    if (val.length > 0) {
      for (String entry : val) {
        write(entry);
      }
    }
  }

  // </editor-fold>

  protected byte[] toByteArray(long val, int size) {
    byte[] tmp = new byte[size];
    for (int x = 0; x < size; x++) {
      tmp[(size - x) - 1] = (byte) (0xff & val);
      val = val >> 8;
    }
    return tmp;
  }

  public abstract void write(byte val) throws IOException;

  public abstract void write(char val) throws IOException;

  public abstract void write(byte[] val) throws IOException;

  protected abstract void write(long val, int size) throws IOException;

}
