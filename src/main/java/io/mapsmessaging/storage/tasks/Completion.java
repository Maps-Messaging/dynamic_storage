package io.mapsmessaging.storage.tasks;


public interface Completion<V> {

  void onCompletion(V result);

  void onException(Exception exception);

}
