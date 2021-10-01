package io.mapsmessaging.storage.impl.file.partition;

import io.mapsmessaging.storage.Storable;
import lombok.Getter;

public class IndexGet<T extends Storable> {

  private final @Getter IndexRecord record;
  private final @Getter T object;

  public IndexGet(IndexRecord record, T object){
    this.record = record;
    this.object = object;
  }
}
