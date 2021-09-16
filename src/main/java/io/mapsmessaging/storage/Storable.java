package io.mapsmessaging.storage;

import io.mapsmessaging.storage.impl.ObjectReader;
import io.mapsmessaging.storage.impl.ObjectWriter;
import java.io.IOException;
import org.jetbrains.annotations.NotNull;

public interface Storable {

  long getKey();

  void read(@NotNull ObjectReader objectReader) throws IOException;

  void write(@NotNull ObjectWriter objectWriter) throws IOException;

}
