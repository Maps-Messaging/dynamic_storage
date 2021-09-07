# Dynamic Storage
Provides a generic API to store, retrieve and manage objects




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
      <version>1.0.0</version>
    </dependency>
```   

[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=dynamic_storage&metric=alert_status)](https://sonarcloud.io/dashboard?id=dynamic_storage)

