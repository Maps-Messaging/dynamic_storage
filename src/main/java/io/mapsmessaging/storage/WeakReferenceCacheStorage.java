package io.mapsmessaging.storage;

import java.io.IOException;
import java.lang.ref.SoftReference;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class WeakReferenceCacheStorage<T extends Storable> implements Storage<T> {

  private final Storage<T> actual;
  private final Map<Long, Cache<T>> cache;

  public WeakReferenceCacheStorage(Storage<T> actual){
    this.actual = actual;
    cache = new WeakHashMap<>();
  }
  @Override
  public void delete() throws IOException {
    cache.clear();
    actual.delete();
  }

  @Override
  public void add(@NotNull T object) throws IOException {
    cache.put(object.getKey(), new Cache<>(object));
    actual.add(object);
  }

  @Override
  public void remove(long key) throws IOException {
    actual.remove(key);
    cache.remove(key);
  }

  @Override
  public @Nullable T get(long key) throws IOException {
    T obj = null;
    Cache<T> cached = cache.get(key);
    if(cached != null){
      obj = cached.objectSoftReference.get();
      if(obj != null){
        return obj;
      }
    }
    obj = actual.get(key);
    if(obj != null && cached != null){
      cached.update(obj);
    }
    return obj;
  }

  @Override
  public long size() throws IOException {
    return actual.size();
  }

  @Override
  public boolean isEmpty() {
    return actual.isEmpty();
  }

  @Override
  public @NotNull List<Long> keepOnly(@NotNull List<Long> listToKeep) {
    cache.clear();
    return actual.keepOnly(listToKeep);
  }

  @Override
  public void close() throws IOException {
    cache.clear();
    actual.close();
  }

  private static final class Cache<T> {
    private SoftReference<T> objectSoftReference;

    Cache(T obj) {
      update(obj);
    }

    public void update(T obj) {
      objectSoftReference = new SoftReference<>(obj);
    }
  }
}
