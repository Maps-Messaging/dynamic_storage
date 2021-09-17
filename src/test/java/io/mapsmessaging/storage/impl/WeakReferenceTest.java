package io.mapsmessaging.storage.impl;

import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.storage.impl.layered.weakReference.WeakReferenceCacheStorage;
import java.io.IOException;

public class WeakReferenceTest extends BaseLayeredTest{

  @Override
  public Storage<MappedData> createStore(Storage<MappedData> storage) throws IOException {
    return new WeakReferenceCacheStorage<>(storage);
  }
}
