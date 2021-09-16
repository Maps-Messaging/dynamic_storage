package io.mapsmessaging.storage.impl.seekableChannel;

import io.mapsmessaging.storage.Factory;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.storage.impl.BaseStorageFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SeekableChannelFactory<T extends Storable> extends BaseStorageFactory<T> {

  public SeekableChannelFactory(){}

  protected SeekableChannelFactory (Map<String, String> properties, Factory<T> factory){
    super(properties, factory);
  }

  @Override
  public String getName() {
    return "SeekableChannel";
  }

  @Override
  public Storage<T> create(String name) throws IOException {
    boolean sync = false;
    if(properties.containsKey("Sync")){
      sync = Boolean.parseBoolean(properties.get("Sync"));
    }
    return new SeekableChannelStorage<>(name, factory, sync);
  }

  @Override
  public List<Storage<T>> discovered() {
    return new ArrayList<>();
  }

}

