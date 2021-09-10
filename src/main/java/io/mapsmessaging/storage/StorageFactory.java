package io.mapsmessaging.storage;

import java.io.IOException;
import java.util.List;
import java.util.Map;


public interface StorageFactory<T extends Storable> {

  String getName();

  Storage<T> create(String name) throws IOException;

  List<Storage<T>> discovered();

  StorageFactory<T> create(Map<String, String> properties, Factory<T> factory);

}
