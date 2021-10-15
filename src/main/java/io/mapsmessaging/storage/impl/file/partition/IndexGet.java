package io.mapsmessaging.storage.impl.file.partition;

import io.mapsmessaging.storage.Storable;
import lombok.Getter;

public class IndexGet<T extends Storable> {

  private final @Getter
  IndexRecord indexRecord;
  private final @Getter
  T object;

  public IndexGet(IndexRecord indexRecord, T object) {
    this.indexRecord = indexRecord;
    this.object = object;
  }
}
