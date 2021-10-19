package io.mapsmessaging.storage;

import io.mapsmessaging.logging.Logger;
import io.mapsmessaging.logging.LoggerFactory;
import io.mapsmessaging.storage.logging.StorageLogMessages;
import java.io.IOException;
import java.util.Queue;

public class BaseExpiredHandler<T extends Storable> implements ExpiredStorableHandler {

  private final Logger logger = LoggerFactory.getLogger(BaseExpiredHandler.class);
  private final Storage<T> storage;

  public BaseExpiredHandler(Storage<T> storage) {
    this.storage = storage;
  }

  @Override
  public void expired(Queue<Long> listOfExpiredEntries) throws IOException {
    for (Long remove : listOfExpiredEntries) {
      if(logger.isTraceEnabled()) logger.log(StorageLogMessages.REMOVING_EXPIRED_ENTRY, remove, storage.getName());
      storage.remove(remove);
    }
  }
}
