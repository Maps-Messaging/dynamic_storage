package io.mapsmessaging.storage;

import io.mapsmessaging.storage.streams.ObjectReader;
import io.mapsmessaging.storage.streams.ObjectWriter;
import org.jetbrains.annotations.NotNull;

public interface Storable {

  long getKey();

  void read(@NotNull ObjectReader objectReader);

  void write(@NotNull ObjectWriter objectWriter);

}
