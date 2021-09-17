package io.mapsmessaging.storage.impl.layered.jcs;

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.storage.impl.layered.BaseLayeredStorage;
import java.io.IOException;
import org.apache.commons.jcs3.JCS;
import org.apache.commons.jcs3.access.CacheAccess;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class JCSCachedStorage<T extends Storable> extends BaseLayeredStorage<T> {

  private final CacheAccess<Long, T> cache;


  public JCSCachedStorage(Storage<T> baseStorage) {
    super(baseStorage);
    cache = JCS.getInstance( baseStorage.getName()+"_cache" );
  }

  @Override
  public void add(@NotNull T object) throws IOException {
    super.add(object);
    cache.put(object.getKey(), object);
  }

  @Override
  public boolean remove(long key) throws IOException {
    cache.remove(key);
    return super.baseStorage.remove(key);
  }

  @Override
  public @Nullable T get(long key) throws IOException {
    T obj = cache.get(key);
    if(obj == null) {
      obj = super.baseStorage.get(key);
      if (obj != null) {
        cache.put(key, obj);
      }
    }
    return obj;
  }

}
