package io.mapsmessaging.storage;

import java.util.Map;
import org.jetbrains.annotations.NotNull;

public abstract class Storage<T extends Storable> implements Map<Long, T> {

  protected final Factory<T> factory;

  public Storage(@NotNull Factory<T> factory){
    this.factory = factory;
  }
}
