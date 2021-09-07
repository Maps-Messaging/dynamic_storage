package io.mapsmessaging.storage;

import java.io.InputStream;
import java.io.OutputStream;

public interface Storable {

  void read(InputStream inputStream);

  void write(OutputStream outputStream);

}
