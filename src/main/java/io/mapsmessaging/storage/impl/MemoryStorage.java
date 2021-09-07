package io.mapsmessaging.storage.impl;

import io.mapsmessaging.storage.Factory;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class MemoryStorage<T extends Storable> extends Storage<T> {

  private final Map<Long, T> memoryMap;

  public MemoryStorage(@NotNull Factory<T> factory) {
    super(factory);
    memoryMap = new LinkedHashMap<>();
  }

  @Override
  public int size() {
    return memoryMap.size();
  }

  @Override
  public boolean isEmpty() {
    return memoryMap.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return memoryMap.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return memoryMap.containsValue(value);
  }

  @Override
  public T get(Object key) {
    return memoryMap.get(key);
  }

  @Nullable
  @Override
  public T put(Long key, T value) {
    return memoryMap.put(key, value);
  }

  @Override
  public T remove(Object key) {
    return memoryMap.remove(key);
  }

  @Override
  public void putAll(@NotNull Map<? extends Long, ? extends T> m) {
    memoryMap.putAll(m);
  }

  @Override
  public void clear() {
    memoryMap.clear();
  }

  @NotNull
  @Override
  public Set<Long> keySet() {
    return memoryMap.keySet();
  }

  @NotNull
  @Override
  public Collection<T> values() {
    return memoryMap.values();
  }

  @NotNull
  @Override
  public Set<Entry<Long, T>> entrySet() {
    return memoryMap.entrySet();
  }
}
