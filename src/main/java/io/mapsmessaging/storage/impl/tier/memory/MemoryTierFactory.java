package io.mapsmessaging.storage.impl.tier.memory;

import io.mapsmessaging.storage.ExpiredStorableHandler;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.StorableFactory;
import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.storage.impl.BaseStorageFactory;
import io.mapsmessaging.storage.impl.file.PartitionStorageFactory;
import io.mapsmessaging.storage.impl.memory.MemoryFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MemoryTierFactory<T extends Storable> extends BaseStorageFactory<T> {

  private static final long MIGRATION_TIME = 60_000;
  private final PartitionStorageFactory<T> partitionStorageFactory;
  private final MemoryFactory<ObjectMonitor<T>> memoryFactory;

  public MemoryTierFactory() {
    partitionStorageFactory = null;
    memoryFactory = null;
  }

  public MemoryTierFactory(Map<String, String> properties, StorableFactory<T> storableFactory, ExpiredStorableHandler expiredHandler) {
    super(properties, storableFactory, expiredHandler);
    partitionStorageFactory = new PartitionStorageFactory<>(properties, storableFactory, expiredHandler);
    memoryFactory = new MemoryFactory<>(properties, new ObjectMonitorFactory<>(), expiredHandler);
  }

  @Override
  public String getName() {
    return "MemoryTier";
  }

  @Override
  public Storage<T> create(String name) throws IOException {
    if (memoryFactory == null || partitionStorageFactory == null) {
      throw new IOException("Uninitialised factory being used.. not supported");
    }
    long migrationTime = MIGRATION_TIME;
    if (properties.containsKey("MigrationPeriod")) {
      migrationTime = Long.parseLong(properties.get("MigrationPeriod"));
    }

    long scanInterval = MIGRATION_TIME;
    if (properties.containsKey("ScanInterval")) {
      scanInterval = Long.parseLong(properties.get("ScanInterval"));
    }

    Storage<ObjectMonitor<T>> memoryStore = memoryFactory.create(name);
    Storage<T> fileStore = partitionStorageFactory.create(name);
    return new MemoryTierStorage<>(memoryStore, fileStore, scanInterval, migrationTime);
  }

  @Override
  public List<Storage<T>> discovered() {
    return new ArrayList<>();
  }

}
