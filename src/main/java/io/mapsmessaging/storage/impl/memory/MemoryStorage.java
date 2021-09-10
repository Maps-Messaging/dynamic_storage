package io.mapsmessaging.storage.impl.memory;

import io.mapsmessaging.storage.Factory;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jetbrains.annotations.NotNull;

public class MemoryStorage<T extends Storable> implements Storage<T> {

  private final Map<Long, T> memoryMap;

  public MemoryStorage(@NotNull Factory<T> factory) {
    memoryMap = new LinkedHashMap<>();
  }

  @Override
  public void delete() throws IOException {
    memoryMap.clear();
  }

  @Override
  public void add(T object) throws IOException {
    memoryMap.put(object.getKey(), object);
  }

  @Override
  public void remove(long key) throws IOException {
    memoryMap.remove(key);
  }

  @Override
  public T get(long key) throws IOException {
    return memoryMap.get(key);
  }

  @Override
  public long size() throws IOException {
    return memoryMap.size();
  }

  @Override
  public boolean isEmpty() {
    return memoryMap.isEmpty();
  }

  @Override
  public @NotNull List<Long> keepOnly(@NotNull List<Long> listToKeep) {
    Set<Long> itemsToRemove = memoryMap.keySet();
    itemsToRemove.removeIf(listToKeep::contains);
    if(!itemsToRemove.isEmpty()){
      for(long key:itemsToRemove){
        memoryMap.remove(key);
      }
    }

    if(itemsToRemove.size() != listToKeep.size()){
      Set<Long> actual = memoryMap.keySet();
      listToKeep.removeIf(actual::contains);
      return listToKeep;
    }
    return new ArrayList<>();
  }

  @Override
  public void close() throws IOException {
    memoryMap.clear();
  }
}
