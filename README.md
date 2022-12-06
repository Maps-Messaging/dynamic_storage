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

Add the repository configuration into the pom.xml
``` xml
    <!-- MapsMessaging jfrog server --> 
    <repository>
      <id>mapsmessaging.io</id>
      <name>artifactory-releases</name>
      <url>https://mapsmessaging.jfrog.io/artifactory/mapsmessaging-mvn-prod</url>
    </repository>
```    

Then include the dependency
``` xml
     <!-- Non Blocking Task Queue module -->
    <dependency>
      <groupId>io.mapsmessaging</groupId>
      <artifactId>DynamicStorage</artifactId>
      <version>2.4.5</version>
    </dependency>
```   

[![SonarCloud](https://sonarcloud.io/images/project_badges/sonarcloud-white.svg)](https://sonarcloud.io/summary/new_code?id=dynamic_storage)
