package io.mapsmessaging.storage.impl;

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import java.io.IOException;
import java.lang.ref.SoftReference;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class WeakReferenceCacheStorage<T extends Storable> extends BaseLayeredStorage<T> {

  private final Map<Long, Cache<T>> cache;

  public WeakReferenceCacheStorage(Storage<T> actual){
    super(actual);
    cache = new WeakHashMap<>();
  }
  @Override
  public void delete() throws IOException {
    super.delete();
    cache.clear();
  }

  @Override
  public void add(@NotNull T object) throws IOException {
    cache.put(object.getKey(), new Cache<>(object));
    super.add(object);
  }

  @Override
  public void remove(long key) throws IOException {
    super.remove(key);
    cache.remove(key);
  }

  @Override
  public @Nullable T get(long key) throws IOException {
    T obj;
    Cache<T> cached = cache.get(key);
    if(cached != null){
      obj = cached.objectSoftReference.get();
      if(obj != null){
        return obj;
      }
    }
    obj = super.get(key);
    if(obj != null && cached != null){
      cached.update(obj);
    }
    return obj;
  }

  @Override
  public @NotNull List<Long> keepOnly(@NotNull List<Long> listToKeep) throws IOException {
    cache.clear();
    return super.keepOnly(listToKeep);
  }

  @Override
  public void close() throws IOException {
    cache.clear();
    super.close();
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
