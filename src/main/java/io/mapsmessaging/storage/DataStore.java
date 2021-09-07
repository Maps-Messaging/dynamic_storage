package io.mapsmessaging.storage;

import java.io.IOException;

public interface DataStore<T extends Storable> {

  boolean contains(long key);

  T read(long key) throws IOException;

  void delete(long key) throws IOException;

  void write(T value) throws IOException;

}
