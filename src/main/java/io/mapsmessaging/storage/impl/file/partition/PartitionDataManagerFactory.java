/*
 *    Copyright [ 2020 - 2024 ] [Matthew Buckton]
 *    Copyright [ 2024 - 2025 ] [Maps Messaging B.V.]
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 */

package io.mapsmessaging.storage.impl.file.partition;

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.impl.file.PartitionStorageConfig;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.ServiceLoader;

@SuppressWarnings({"java:S6548", "java:S3740"})
public class PartitionDataManagerFactory<T extends Storable> {

  private static class Holder {
    static final PartitionDataManagerFactory INSTANCE = new PartitionDataManagerFactory<>();
  }

  public static PartitionDataManagerFactory getInstance() {
    return Holder.INSTANCE;
  }

  private final Map<String, DataStorageFactory<T>> dataStorageManagers;

  public PartitionDataManagerFactory(){
    ServiceLoader<DataStorageFactory> serviceLoader = ServiceLoader.load(DataStorageFactory.class);
    dataStorageManagers = new LinkedHashMap<>();
    for(DataStorageFactory<T> dataStorageFactory:serviceLoader){
      dataStorageManagers.put(dataStorageFactory.getName(), dataStorageFactory);
    }
  }

  public ArchivedDataStorage<T> create(PartitionStorageConfig<T> config) throws IOException {
    String archiveName = "None";
    if(config != null){
      archiveName = config.getArchiveName();
    }
    DataStorageFactory<T> factory = dataStorageManagers.get(archiveName);
    return factory.create(config);
  }
}
