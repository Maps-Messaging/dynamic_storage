package io.mapsmessaging.storage.impl.tier.memory;

import io.mapsmessaging.storage.Storable;
import lombok.Getter;
import lombok.Setter;

class ObjectMonitor<T extends Storable> implements Storable {

  private final T storable;

  private @Getter
  @Setter
  long lastAccess;

  ObjectMonitor() {
    storable = null;
  }

  public ObjectMonitor(T storable) {
    this.storable = storable;
    lastAccess = System.currentTimeMillis();
  }

  public T getStorable() {
    return storable;
  }

  @Override
  public long getKey() {
    return storable.getKey();
  }

  @Override
  public long getExpiry() {
    return storable.getExpiry();
  }
}