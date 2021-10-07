package io.mapsmessaging.storage;

import java.io.IOException;
import java.util.List;

@FunctionalInterface
public interface ExpiredStorableHandler<T extends Storable> {

   void expired(Storage<T> storage, List<Long> listOfExpiredEntries) throws IOException;

}
