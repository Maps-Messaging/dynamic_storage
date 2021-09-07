package io.mapsmessaging.storage;

import java.util.Map;

public abstract class Storage<T extends Storable> implements Map<Long, T> {

  protected final Factory<T> factory;

  public Storage(Factory<T> factory){
    this.factory = factory;
  }
}
