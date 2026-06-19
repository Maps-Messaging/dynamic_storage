# Dynamic Storage

Dynamic Storage is a generic Java storage library for objects keyed by a 64-bit `long`.

It provides pluggable storage backends for in-memory, file-backed, partitioned, tiered, cached, and archived storage. It is designed for systems that need predictable key ordering, persistent recovery, and configurable storage behaviour without tying the application to a single storage implementation.

Dynamic Storage is used by Maps Messaging for durable event and message storage.

## Features

* Generic `Storage<T>` API for objects implementing `Storable`
* 64-bit long key support
* Natural numeric key ordering
* In-memory storage for fast access
* File-backed persistent storage
* Partitioned file storage for large datasets
* Tiered memory/file storage
* Cache support using JCS or weak references
* Deferred/archive storage support

    * compressed files
    * alternative directories
    * S3-backed archive storage
* Expiry-aware records
* Synchronous and asynchronous task support
* Recovery validation for unclean partition reopen
* Configurable storage behaviour through properties

## Storage Model

Objects stored in Dynamic Storage must implement `Storable`.

Each stored object has a unique `long` key. Keys are ordered using natural numeric ordering, so lower keys are treated as older records and higher keys as newer records.

This is useful for event streams, queues, message stores, and other systems where delivery or replay order matters.

## Quick Start

Create a `Storage<T>` using `StorageBuilder`.

```java
Map<String, String> properties = new LinkedHashMap<>();
properties.put("storeType", "Partition");
properties.put("Sync", "false");
properties.put("ItemCount", "1000");
properties.put("ExpiredEventPoll", "120");
properties.put("MaxPartitionSize", String.valueOf(1024L * 1024L * 1024L));

StorageBuilder<MyData> storageBuilder = new StorageBuilder<>();
storageBuilder
    .setFactory(myDataFactory)
    .setName("data/store")
    .setProperties(properties);

Storage<MyData> storage = storageBuilder.build();

MyData record = new MyData(1L, "Hello, World!");
storage.add(record);

MyData retrieved = storage.get(1L);

storage.close();
```

Your data type must implement `Storable`, and you must provide a matching factory capable of packing and unpacking your object type.

## Example `Storable`

The exact implementation depends on your object model and factory, but a stored object must expose a stable key.

```java
public class MyData implements Storable {

  private final long key;
  private final String value;

  public MyData(long key, String value) {
    this.key = key;
    this.value = value;
  }

  @Override
  public long getKey() {
    return key;
  }

  public String getValue() {
    return value;
  }
}
```

Serialization and deserialization are handled by the configured `StorableFactory<T>`.

## Building a Partition Store

Partition storage is useful when records should be persisted across multiple file-backed partitions.

```java
static Storage<MappedData> buildPartitionStore(
    StorableFactory<MappedData> factory,
    Map<String, String> properties,
    String storeName) throws IOException {

  StorageBuilder<MappedData> storageBuilder = new StorageBuilder<>();
  storageBuilder
      .setFactory(factory)
      .setName("data" + File.separator + storeName)
      .setProperties(properties);

  return storageBuilder.build();
}
```

Example properties:

```java
Map<String, String> properties = new LinkedHashMap<>();
properties.put("storeType", "Partition");
properties.put("Sync", "false");
properties.put("ItemCount", "1000");
properties.put("ExpiredEventPoll", "120");
properties.put("MaxPartitionSize", String.valueOf(1024L * 1024L * 1024L));
```

## Configuration

Dynamic Storage is configured using string properties supplied to `StorageBuilder`.

Common configuration areas include:

| Area           | Description                                                                   |
| -------------- | ----------------------------------------------------------------------------- |
| Storage type   | Selects memory, file, partition, or tiered storage                            |
| Sync mode      | Controls whether file writes use synchronous disk options                     |
| Partition size | Controls when file-backed partitions roll over                                |
| Item count     | Controls index capacity per partition                                         |
| Expiry polling | Controls how frequently expired records are scanned                           |
| Cache          | Enables JCS or weak-reference caching                                         |
| Archive        | Enables deferred/archive storage such as compressed files, directories, or S3 |

See [`configuration.md`](configuration.md) for the full list of supported properties.

## Recovery Behaviour

Partition storage tracks whether index and data files were closed cleanly.

On unclean reopen, the store validates indexed records against the data file. Valid records are retained. Invalid indexed records, such as records pointing to truncated or corrupt data, are removed from the live index.

Unreferenced trailing bytes in the data file are ignored.

This allows the store to recover from partial writes while preserving valid records.

## Maven Dependency

Add the dependency to your project:

```xml
<dependency>
  <groupId>io.mapsmessaging</groupId>
  <artifactId>dynamic-storage</artifactId>
  <version>VERSION</version>
</dependency>
```

Replace `VERSION` with the release version you want to use.

Check the published artifact name before release. If the project currently publishes as `DynamicStorage`, update this section to match the actual Maven artifact.

## API Documentation

API documentation will be published with the project site.

Until then, the main entry points are:

* `Storage<T>`
* `StorageBuilder<T>`
* `Storable`
* `StorableFactory<T>`

## Contributing

Contributions are welcome.

Before submitting a change:

1. Add or update tests for the behaviour being changed.
2. Run the full test suite.
3. Keep storage format and recovery compatibility in mind.
4. Avoid changing persisted file formats unless migration behaviour is clearly defined.

## License

Dynamic Storage is licensed under the terms defined in the repository [`LICENSE`](LICENSE) file.

The source headers currently refer to the Apache License 2.0 with the Commons Clause License Condition. Ensure this section matches the actual repository license before publishing.

## Project Status

Dynamic Storage is part of the Maps Messaging ecosystem and is actively used in storage, caching, tiering, and archive workflows.

[![SonarCloud](https://sonarcloud.io/images/project_badges/sonarcloud-white.svg)](https://sonarcloud.io/summary/new_code?id=dynamic_storage)
