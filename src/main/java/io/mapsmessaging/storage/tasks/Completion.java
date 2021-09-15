package io.mapsmessaging.storage.tasks;


@FunctionalInterface
public interface Completion<V> {
  void complete(V result);
}
