package io.mapsmessaging.storage;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class StorageFactoryFactory {

  private static final StorageFactoryFactory instance = new StorageFactoryFactory();
  public static StorageFactoryFactory getInstance(){
    return instance;
  }

  private final ServiceLoader<StorageFactory> storageFactories;

  public List<String> getKnown(){
    List<String> known = new ArrayList<>();
    storageFactories.forEach(storageFactory -> known.add(storageFactory.getName()));
    return known;
  }

  @SneakyThrows
  @Nullable public <T extends Storable>  StorageFactory<T> create(@NotNull String name,@NotNull Map<String, String> properties, @NotNull Factory<T> factory){
    Optional<Provider<StorageFactory>> first = storageFactories.stream().filter(storageFactoryProvider -> storageFactoryProvider.get().getName().equals(name)).findFirst();
    if(first.isPresent()){
      StorageFactory<?> found = first.get().get();
      Class<T> clazz = (Class<T>) found.getClass();
      Constructor<T>[] constructors = (Constructor<T>[]) clazz.getDeclaredConstructors();
      Constructor<T> constructor = null;
      for (Constructor<T> cstr : constructors) {
        if (cstr.getParameters().length == 2) {
          constructor = cstr;
          break;
        }
      }
      if (constructor != null) {
        constructor.setAccessible(true);
        return (StorageFactory<T>) constructor.newInstance(properties, factory);
      }
    }
    return null;
  }

  private StorageFactoryFactory(){
    storageFactories = ServiceLoader.load(StorageFactory.class);
  }

}
