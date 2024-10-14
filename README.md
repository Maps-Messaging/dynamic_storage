# Dynamic Storage

Provides a generic API to store, retrieve, and manage objects that are keyed via a 64-bit long.

## Features

- Caching layer with JCS and Weak References.
- Memory Store for high-speed access.
- File Store for persistent storage.
- Tiered storage combining memory and file systems.
- Archiving to S3, compressed files, or alternative directories.
- Asynchronous API supporting non-blocking operations.

## Quick Start

To get started with Dynamic Storage, your objects need to implement the `Storable` interface. Here is an example of storing and retrieving a `Storable` object:

```java
// Java code illustrating the creation, storage, and retrieval of a Storable object
class MyStorable implements Storable {
private final Long id;
private final byte[] data;

public MyStorable(Long id, byte[] data) {
this.id = id;
this.data = data;
}

@Override
public Long getId() {
return id;
}

@Override
public byte[] toBytes() {
return data;
}

@Override
public void fromBytes(byte[] bytes) {
// Implementation to convert bytes to object's fields
}
}

DynamicStorage storage = new DynamicStorage();
MyStorable storable = new MyStorable(1L, "Hello, World!".getBytes());
Long key = storage.store(storable);
MyStorable retrieved = (MyStorable) storage.retrieve(key);
```

Ensure to replace `DynamicStorage` with the actual class name from your library that interacts with the storage system.

## Building a DynamicStore

To create a `DynamicStore`, you can use the `StorageBuilder` with the desired configurations:

```java
// Example method to build a DynamicStore with custom properties
static Storage<MappedData> build(Map<String, String> properties, String testName) throws IOException {
StorageBuilder<MappedData> storageBuilder = new StorageBuilder<>();
storageBuilder.setStorageType("Partition")
.setFactory(getFactory())
.setName("test_file" + File.separator + testName)
.setProperties(properties);
return storageBuilder.build();
}
```

Replace `MappedData` with your specific data type that implements `Storable` and provide the necessary properties to configure the storage as per your requirements.


## Configuration

The Dynamic Storage can be customized with various configurations for caching, archiving, and storage management. Here's a brief overview of how to configure key features:

- **Caching**: Enable with properties for JCS or weak references.
- **Archiving**: Set up archiving to S3 buckets, compressed files, or alternative directories.
- **Storage**: Configure memory and file stores with backup options.

For detailed configuration instructions, please see our [Configuration Guide](configuration.md).

## API Documentation

Coming soon on GitHub Pages.

## Contributing

How to contribute to the Dynamic Storage project.

## License

The Maps Messaging Dynamic Storage is dual-licensed under the Mozilla Public License Version 2.0 (MPL 2.0) and the Apache License 2.0 with Commons Clause License Condition v1.0.

Under the MPL 2.0 license, the software is provided for use, modification, and distribution under the terms of the MPL 2.0.

Additionally, the "Commons Clause" restricts the selling of the software, which means you may not sell the software or services whose value derives entirely or substantially from the software's functionality.

For full license terms, see the [LICENSE](LICENSE) file in the repository.



Include the Maven dependency to use Dynamic Storage in your project:

```xml
<!-- Dynamic Storage library -->
<dependency>
  <groupId>io.mapsmessaging</groupId>
  <artifactId>DynamicStorage</artifactId>
</dependency>
```

For a full guide on getting started, configuration, and extending the library, please see the [Wiki](LINK_TO_WIKI) or [GitHub Pages](LINK_TO_GITHUB_PAGES) for detailed documentation.

[![SonarCloud](https://sonarcloud.io/images/project_badges/sonarcloud-white.svg)](https://sonarcloud.io/summary/new_code?id=dynamic_storage)

[![Mutable.ai Auto Wiki](https://img.shields.io/badge/Auto_Wiki-Mutable.ai-blue)](https://wiki.mutable.ai/Maps-Messaging/dynamic_storage)