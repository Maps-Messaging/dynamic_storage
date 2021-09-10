package io.mapsmessaging.storage.impl.mapDb;

import io.mapsmessaging.storage.Factory;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.storage.StorageFactory;
import io.mapsmessaging.storage.impl.BaseStorageFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MapDBFactory<T extends Storable> extends BaseStorageFactory<T> {

  public MapDBFactory(){}

  protected MapDBFactory (Map<String, String> properties, Factory<T> factory){
    super(properties, factory);
  }


  @Override
  public String getName() {
    return "MapDB";
  }

  @Override
  public Storage<T> create(String name) {
    return new MapDBStorage<>(properties.get("basePath"), name, factory);
  }

  @Override
  public List<Storage<T>> discovered() {
    return new ArrayList<>();
  }

  @Override
  public StorageFactory<T> create(Map<String, String> properties, Factory<T> factory) {
    return new MapDBFactory<>(properties, factory);
  }
}
