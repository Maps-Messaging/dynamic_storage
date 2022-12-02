package io.mapsmessaging.storage.impl;

import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.storage.StorageBuilder;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.Map;

public class BasePartitionStoreTest extends BaseStoreTest {

  @Override
  public Storage<MappedData> createStore(String testName, boolean sync) throws IOException {
    return build(testName, sync);
  }

  public static Map<String, String> buildProperties(boolean sync) throws IOException {
    File file = new File("test_file" + File.separator);
    if (!file.exists()) {
      Files.createDirectory(file.toPath());
    }
    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("Sync", "" + sync);
    properties.put("ItemCount", "" + 100);
    properties.put("MaxPartitionSize", "" + (512L * 1024L * 1024L)); // set to 5MB data limit
    return properties;
  }

  static Storage<MappedData> build(Map<String, String> properties, String testName) throws IOException {
    StorageBuilder<MappedData> storageBuilder = new StorageBuilder<>();
    storageBuilder.setStorageType("Partition")
        .setFactory(getFactory())
        .setName("test_file" + File.separator + testName)
        .setProperties(properties);
    return storageBuilder.build();
  }

  static Storage<MappedData> build(String testName, boolean sync) throws IOException {
    Map<String, String> properties = buildProperties(sync);
    return build(properties, testName);
  }

}