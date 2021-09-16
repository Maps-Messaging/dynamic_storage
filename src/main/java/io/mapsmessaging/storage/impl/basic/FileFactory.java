package io.mapsmessaging.storage.impl.basic;

import io.mapsmessaging.storage.Factory;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.storage.impl.BaseStorageFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FileFactory <T extends Storable> extends BaseStorageFactory<T> {

  public FileFactory() {
  }

  protected FileFactory(Map<String, String> properties, Factory<T> factory) {
    super(properties, factory);
  }

  @Override
  public String getName() {
    return "File";
  }

  @Override
  public Storage<T> create(String name) throws IOException {
    return new FileStorage<>(name, factory);
  }

  @Override
  public List<Storage<T>> discovered() {
    return new ArrayList<>();
  }

}
