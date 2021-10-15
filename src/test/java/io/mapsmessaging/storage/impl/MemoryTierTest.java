package io.mapsmessaging.storage.impl;

import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.storage.StorageBuilder;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class MemoryTierTest extends BaseStoreTest {

  public static Storage<MappedData> build(String testName, boolean sync) throws IOException {
    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("Sync", "" + sync);
    StorageBuilder<MappedData> storageBuilder = new StorageBuilder<>();
    storageBuilder.setStorageType("MemoryTier")
        .setFactory(getFactory())
        .setCache()
        .setName(testName)
        .setProperties(properties);
    return storageBuilder.build();
  }

  @Override
  public Storage<MappedData> createStore(String testName, boolean sync) throws IOException {
    return build(testName, sync);
  }
}
