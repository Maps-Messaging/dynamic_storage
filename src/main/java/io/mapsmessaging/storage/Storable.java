package io.mapsmessaging.storage;

import java.io.InputStream;
import java.io.OutputStream;
import org.jetbrains.annotations.NotNull;

public interface Storable {


  void read(@NotNull InputStream inputStream);

  void write(@NotNull OutputStream outputStream);

}
