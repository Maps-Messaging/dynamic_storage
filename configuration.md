## Configuration

Dynamic Storage is configured using string properties supplied to `StorageBuilder`.

```java
Map<String, String> properties = new LinkedHashMap<>();
properties.put("storeType", "Partition");
properties.put("Sync", "false");
properties.put("ItemCount", "1000");
properties.put("ExpiredEventPoll", "120");
properties.put("MaxPartitionSize", String.valueOf(1024L * 1024L * 1024L));

StorageBuilder<MappedData> storageBuilder = new StorageBuilder<>();
storageBuilder
    .setFactory(getFactory())
    .setName("data" + File.separator + "example-store")
    .setProperties(properties);

Storage<MappedData> storage = storageBuilder.build();
```

### Partition Store Configuration

Partition storage persists records to file-backed partitions. Each partition contains an index file and a data file.

```java
Map<String, String> properties = new LinkedHashMap<>();
properties.put("storeType", "Partition");
properties.put("Sync", "false");
properties.put("ItemCount", "1000");
properties.put("ExpiredEventPoll", "120");
properties.put("MaxPartitionSize", String.valueOf(1024L * 1024L * 1024L));
```

Common partition properties:

| Property           | Description                                                                       |
| ------------------ | --------------------------------------------------------------------------------- |
| `storeType`        | Selects the storage implementation. Use `Partition` for partitioned file storage. |
| `Sync`             | Enables synchronous file writes when set to `true`.                               |
| `ItemCount`        | Number of indexed records per partition.                                          |
| `ExpiredEventPoll` | Expiry scan interval, in seconds.                                                 |
| `MaxPartitionSize` | Maximum data size for a partition before rollover.                                |

### Cache Configuration

Dynamic Storage supports cache layers, depending on the configured cache implementation.

Cache configuration is supplied through the same property map used by `StorageBuilder`.

```java
Map<String, String> properties = new LinkedHashMap<>();
properties.put("storeType", "Partition");
properties.put("Sync", "false");
properties.put("ItemCount", "1000");
properties.put("ExpiredEventPoll", "120");
properties.put("MaxPartitionSize", String.valueOf(1024L * 1024L * 1024L));

// Add cache-specific properties supported by the selected cache implementation.

Storage<MappedData> storage = buildPartitionStore(getFactory(), properties, "cached-store");
```

### Tiered Storage Configuration

Tiered storage combines fast memory access with persistent backing storage.

Use tiered storage when recently accessed records should remain in memory while older or less frequently accessed records can be migrated to a backing store.

```java
Map<String, String> properties = new LinkedHashMap<>();
properties.put("storeType", "MemoryTier");
properties.put("Sync", "false");
properties.put("ItemCount", "1000");
properties.put("ExpiredEventPoll", "120");
properties.put("MaxPartitionSize", String.valueOf(1024L * 1024L * 1024L));

// Add tier-specific properties such as memory limits and migration timing.

Storage<MappedData> storage = buildPartitionStore(getFactory(), properties, "tiered-store");
```

### Archive Configuration

Deferred archive storage can move full or inactive partition data to another location, such as compressed files, an archive directory, or S3, depending on the configured archive implementation.

```java
Map<String, String> properties = new LinkedHashMap<>();
properties.put("storeType", "Partition");
properties.put("Sync", "false");
properties.put("ItemCount", "1000");
properties.put("ExpiredEventPoll", "120");
properties.put("MaxPartitionSize", String.valueOf(1024L * 1024L * 1024L));

// Add archive-specific properties supported by the selected deferred storage implementation.

Storage<MappedData> storage = buildPartitionStore(getFactory(), properties, "archived-store");
```

### Notes

Property names are implementation-specific. Use the property names supported by the selected storage, cache, tier, or archive implementation.

For production storage, choose `Sync=true` when durability on crash is more important than write throughput. Use `Sync=false` when throughput is more important and the application can tolerate recovery from the last cleanly written records.
