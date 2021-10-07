package io.mapsmessaging.storage;

import java.io.IOException;
import java.util.Queue;

@FunctionalInterface
public interface ExpiredStorableHandler<T extends Storable> {

   void expired(Queue<Long>  listOfExpiredEntries) throws IOException;

}
