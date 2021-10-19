package io.mapsmessaging.storage.impl;

import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.storage.StorageBuilder;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

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


  @Test
  void sizeLimitTest() throws IOException {
    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("Sync", "false");
    properties.put("Tier1Size", "100");
    StorageBuilder<MappedData> storageBuilder = new StorageBuilder<>();
    storageBuilder.setStorageType("MemoryTier")
        .setFactory(getFactory())
        .setCache()
        .setName(testName)
        .setProperties(properties);
    Storage<MappedData> storage = storageBuilder.build();
    try {
      for (int x = 0; x < 100; x++) {
        storage.add(createMessageBuilder(x));
      }

      // There should be zero events in the file but 100 in the memory tier

      for (int x = 100; x < 200; x++) {
        storage.add(createMessageBuilder(x));
      }
      // Now there should be 100 in both
      storage.size();
    } finally {
      storage.delete();
    }

  }
}
