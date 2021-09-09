package io.mapsmessaging.storage;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface Storage<T extends Storable> extends Closeable {

  void delete() throws IOException;

  void add(@NotNull T object) throws IOException;

  void remove(long key) throws IOException;

  @Nullable T get(long key) throws IOException;

  long size() throws IOException;

  boolean isEmpty();

  // Returns a list of events NOT found but was in the to keep list
  @NotNull List<Long> keepOnly(@NotNull List<Long> listToKeep);

}
