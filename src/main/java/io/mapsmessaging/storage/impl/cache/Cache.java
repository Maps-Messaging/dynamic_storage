package io.mapsmessaging.storage.impl.cache;

public interface Cache<T> {

  T cacheGet(long key);

  void cachePut(T obj);

  void cacheRemove(long key);

  void cacheClear();

  void cacheDelete();
}
