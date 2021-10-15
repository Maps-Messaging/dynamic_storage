package io.mapsmessaging.storage;

import java.io.IOException;
import java.util.Queue;

public class BaseExpiredHandler<T extends Storable> implements ExpiredStorableHandler {

  private final Storage<T> storage;

  public BaseExpiredHandler(Storage<T> storage) {
    this.storage = storage;
  }

  @Override
  public void expired(Queue<Long> listOfExpiredEntries) throws IOException {
    for (Long remove : listOfExpiredEntries) {
      storage.remove(remove);
    }
  }
}
