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
      <version>2.0.0</version>
    </dependency>
```   

[![SonarCloud](https://sonarcloud.io/images/project_badges/sonarcloud-white.svg)](https://sonarcloud.io/summary/new_code?id=dynamic_storage)
