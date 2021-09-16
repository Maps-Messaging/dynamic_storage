package io.mapsmessaging.storage;

@FunctionalInterface
public interface Factory<T extends Storable> {

  T create();

}
