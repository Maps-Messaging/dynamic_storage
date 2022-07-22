package io.mapsmessaging.storage.impl.file;

import io.mapsmessaging.storage.ExpiredStorableHandler;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.StorableFactory;
import lombok.Getter;
import lombok.Setter;

public class PartitionStorageConfig<T extends Storable> {

  @Getter
  @Setter
  private String fileName;

  @Getter
  @Setter
  private StorableFactory<T> storableFactory;

  @Getter
  @Setter
  private ExpiredStorableHandler expiredHandler;

  @Getter
  @Setter
  private boolean sync;

  @Getter
  @Setter
  private int itemCount;

  @Getter
  @Setter
  private long maxPartitionSize;

  @Getter
  @Setter
  private int expiredEventPoll;

  @Getter
  @Setter
  private TaskQueue taskQueue;

}
