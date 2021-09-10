package io.mapsmessaging.storage;

import io.mapsmessaging.storage.impl.streams.ObjectReader;
import io.mapsmessaging.storage.impl.streams.ObjectWriter;
import org.jetbrains.annotations.NotNull;

public interface Storable {

  long getKey();

  void read(@NotNull ObjectReader objectReader);

  void write(@NotNull ObjectWriter objectWriter);

}
