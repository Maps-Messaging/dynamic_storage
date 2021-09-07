package io.mapsmessaging.storage;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class Storage<T extends Storable> implements Map<Long, T> {

  private final Factory<T> factory;

  public Storage(Factory<T> factory){
    this.factory = factory;
  }

  @Override
  public int size() {
    return 0;
  }

  @Override
  public boolean isEmpty() {
    return false;
  }

  @Override
  public boolean containsKey(Object key) {
    return false;
  }

  @Override
  public boolean containsValue(Object value) {
    return false;
  }

  @Override
  public T get(Object key) {
    return null;
  }

  @Nullable
  @Override
  public T put(Long key, T value) {
    return null;
  }

  @Override
  public T remove(Object key) {
    return null;
  }

  @Override
  public void putAll(@NotNull Map<? extends Long, ? extends T> m) {

  }

  @Override
  public void clear() {

  }

  @NotNull
  @Override
  public Set<Long> keySet() {
    return null;
  }

  @NotNull
  @Override
  public Collection<T> values() {
    return null;
  }

  @NotNull
  @Override
  public Set<Entry<Long, T>> entrySet() {
    return null;
  }
}
