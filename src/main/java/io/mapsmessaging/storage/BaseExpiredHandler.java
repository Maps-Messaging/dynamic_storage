package io.mapsmessaging.storage;

import java.io.IOException;
import java.util.List;

public class BaseExpiredHandler<T extends Storable> implements ExpiredStorableHandler<T> {


  @Override
  public void expired(Storage<T> storage, List<Long> listOfExpiredEntries) throws IOException {
    for(Long remove:listOfExpiredEntries){
      storage.remove(remove);
    }
  }
}
