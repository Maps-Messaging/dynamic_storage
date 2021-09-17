package io.mapsmessaging.storage.impl.layered.weakReference;

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.storage.impl.layered.BaseLayeredStorage;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class WeakReferenceCacheStorage<T extends Storable> extends BaseLayeredStorage<T> {

  private final Map<Long, T> cache;

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
    cache.put(object.getKey(), object);
    super.add(object);
  }

  @Override
  public boolean remove(long key) throws IOException {
    super.remove(key);
    cache.remove(key);
    return true;
  }

  @Override
  public @Nullable T get(long key) throws IOException {
    T obj =  cache.get(key);
    if(obj == null) {
      obj = super.get(key);
      if (obj != null) {
        cache.put(key, obj);
      }
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
}
