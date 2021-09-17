package io.mapsmessaging.storage.impl;

import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.storage.impl.layered.jcs.JCSCachedStorage;
import java.io.IOException;

public class JCSCacheTest extends BaseLayeredTest{

  @Override
  public Storage<MappedData> createStore(Storage<MappedData> storage) throws IOException {
    return new JCSCachedStorage<>(storage);
  }
}
