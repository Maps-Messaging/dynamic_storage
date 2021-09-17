package io.mapsmessaging.storage.impl.layered;

import io.mapsmessaging.storage.LayeredStorage;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import java.io.IOException;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class BaseLayeredStorage<T extends Storable> implements LayeredStorage<T> {


  protected final Storage<T> baseStorage;

  protected BaseLayeredStorage(Storage<T> baseStorage) {
    this.baseStorage = baseStorage;
  }

  @Override
  public String getName() {
    return baseStorage.getName();
  }

  @Override
  public void delete() throws IOException {
    baseStorage.delete();
  }

  @Override
  public void add(@NotNull T object) throws IOException {
    baseStorage.add(object);
  }

  @Override
  public boolean remove(long key) throws IOException {
    return baseStorage.remove(key);
  }

  @Override
  public @Nullable T get(long key) throws IOException {
    return baseStorage.get(key);
  }

  @Override
  public long size() throws IOException {
    return baseStorage.size();
  }

  @Override
  public boolean isEmpty() {
    return baseStorage.isEmpty();
  }

  @Override
  public @NotNull List<Long> keepOnly(@NotNull List<Long> listToKeep) throws IOException {
    return baseStorage.keepOnly(listToKeep);
  }

  @Override
  public void close() throws IOException {
    baseStorage.close();
  }


}
