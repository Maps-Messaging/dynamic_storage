/*
 *   Copyright [2020 - 2022]   [Matthew Buckton]
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
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

package io.mapsmessaging.storage.impl.file.partition.base;

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.StorableFactory;
import io.mapsmessaging.storage.impl.file.PartitionStorageConfig;
import io.mapsmessaging.storage.impl.file.partition.ArchivedDataStorage;
import io.mapsmessaging.storage.impl.file.partition.DataStorageFactory;
import java.io.IOException;

public class BaseDataStorageFactory<T extends Storable>  implements DataStorageFactory<T> {


  public BaseDataStorageFactory(){
    // Only needed for service loading
  }

  @Override
  public String getName() {
    return "None";
  }

  @Override
  public ArchivedDataStorage<T> create(PartitionStorageConfig<T> config, String fileName, StorableFactory<T> storableFactory, boolean sync, long maxPartitionSize)
      throws IOException {
    return new BaseDataStorage<>(fileName, storableFactory, sync, maxPartitionSize);
  }
}
