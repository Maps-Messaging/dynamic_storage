package io.mapsmessaging.storage;

import java.io.IOException;
import java.util.List;

public interface StorageFactory<T extends Storable> {

  String getName();

  Storage<T> create(String name) throws IOException;

  List<Storage<T>> discovered();

}
