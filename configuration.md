## Configuration

### Caching Configuration
To enable caching with JCS:
```java
// Example configuration for JCS caching
Properties properties = new Properties();
properties.put("jcs.enabled", "true");
// Additional JCS-specific properties...
Storage<MappedData> storage = build(properties, "JCSExample");
```

### Archiving Configuration
To archive to an S3 bucket:
```java
// Example configuration for S3 archiving
Properties properties = new Properties();
properties.put("archive.s3.bucketName", "your-bucket-name");
// Additional S3-specific properties...
Storage<MappedData> storage = build(properties, "S3Example");
```

### Memory and File Store Configuration
To set up a memory store with file backup:
```java
// Example configuration for memory and file storage
Properties properties = new Properties();
properties.put("storage.memory.enabled", "true");
properties.put("storage.file.path", "/path/to/backup");
Storage<MappedData> storage = build(properties, "MemoryFileExample");
```
