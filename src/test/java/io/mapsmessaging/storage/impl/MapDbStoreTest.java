package io.mapsmessaging.storage.impl;

import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.storage.StorageFactoryFactory;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class MapDbStoreTest extends BaseStoreTest{

  @Override
  public Storage<MappedData> createStore(boolean sync) throws IOException {
    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("Sync", ""+sync);
    properties.put("basePath", "./test.db");
    return Objects.requireNonNull(StorageFactoryFactory.getInstance().create("MapDB", properties, getFactory())).create("Test");
  }
}
