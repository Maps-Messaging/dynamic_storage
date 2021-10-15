package io.mapsmessaging.storage.impl;

import io.mapsmessaging.storage.AsyncStorage;
import io.mapsmessaging.storage.Storage;
import java.io.IOException;

public class JCSCacheAsyncTest extends BaseAsyncStoreTest {

  @Override
  public AsyncStorage<MappedData> createAsyncStore(String testName, boolean sync) throws IOException {
    Storage<MappedData> storage = JCSCacheTest.build(testName, sync);
    return new AsyncStorage<>(storage);
  }

}
