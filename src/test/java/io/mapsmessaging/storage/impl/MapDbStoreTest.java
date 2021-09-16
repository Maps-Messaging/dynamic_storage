package io.mapsmessaging.storage.impl;

import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.storage.StorageFactoryFactory;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class MapDbStoreTest extends BaseStoreTest{

  @Override
  public Storage<MappedData> createStore() throws IOException {
    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("basePath", "./test.db");
    return StorageFactoryFactory.getInstance().create("MapDB", properties, getFactory()).create("Test");
  }
}
