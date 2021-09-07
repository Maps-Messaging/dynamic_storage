package io.mapsmessaging.storage;

public interface Factory<T extends Storable> {

  T create();

}
