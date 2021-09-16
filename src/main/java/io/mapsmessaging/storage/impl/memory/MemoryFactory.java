package io.mapsmessaging.storage.impl.memory;

import io.mapsmessaging.storage.Factory;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.storage.impl.BaseStorageFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MemoryFactory<T extends Storable> extends BaseStorageFactory<T> {

  public MemoryFactory() {
  }

  protected MemoryFactory(Map<String, String> properties, Factory<T> factory) {
    super(properties, factory);
  }

  @Override
  public String getName() {
    return "Memory";
  }

  @Override
  public Storage<T> create(String name) {
    return new MemoryStorage<>(factory);
  }

  @Override
  public List<Storage<T>> discovered() {
    return new ArrayList<>();
  }

}
