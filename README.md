# Dynamic Storage
Provides a generic API to store, retrieve and manage objects that are keyed via a 64 bit long.

Supports

- Caching layer - JCS, Weak Reference
- Memory Store 
- File Store
- Storage tier of memory and file
- Archiving and automatic restoration of idle data to 
  - S3 bucket
  - Compressed files
  - Alternative directory ( SSD -> HDD or local to network drive)

The storage API supports asynchronous access, meaning you can request an operation and a completion task or wait on a java Future for the operation to complete.


# pom.xml setup

All MapsMessaging libraries are hosted on the [maven central server.](https://central.sonatype.com/search?smo=true&q=mapsmessaging)

Include the dependency
``` xml
     <!-- Non Blocking Task Queue module -->
    <dependency>
      <groupId>io.mapsmessaging</groupId>
      <artifactId>DynamicStorage</artifactId>
      <version>2.4.6</version>
    </dependency>
```   

[![SonarCloud](https://sonarcloud.io/images/project_badges/sonarcloud-white.svg)](https://sonarcloud.io/summary/new_code?id=dynamic_storage)
