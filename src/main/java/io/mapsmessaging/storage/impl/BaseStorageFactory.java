package io.mapsmessaging.storage.impl;

import io.mapsmessaging.storage.Factory;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.StorageFactory;
import java.util.Map;

public abstract class BaseStorageFactory<T extends Storable> implements StorageFactory<T> {

  protected Map<String, String> properties;
  protected Factory<T> factory;

  public BaseStorageFactory() {
  }

  protected BaseStorageFactory(Map<String, String> properties, Factory<T> factory) {
    this.properties = properties;
    this.factory = factory;
  }

}
