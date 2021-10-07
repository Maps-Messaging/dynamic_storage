/*
 *
 * Copyright [2020 - 2021]   [Matthew Buckton]
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 *
 */

package io.mapsmessaging.storage.impl.file;

import io.mapsmessaging.storage.BaseExpiredHandler;
import io.mapsmessaging.storage.ExpiredStorableHandler;
import io.mapsmessaging.storage.StorableFactory;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.storage.impl.BaseStorageFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PartitionStorageFactory<T extends Storable> extends BaseStorageFactory<T> {
  private static final int ITEM_COUNT = 524288;
  private static final long MAXIMUM_DATA_SIZE = (1L << 32); //4GB by default
  private static final int EXPIRED_EVENT_MONITOR_TIME = 1;

  public PartitionStorageFactory() {
  }

  public PartitionStorageFactory(Map<String, String> properties, StorableFactory<T> storableFactory, ExpiredStorableHandler<T> expiredHandler) {
    super(properties, storableFactory, expiredHandler);
  }

  @Override
  public String getName() {
    return "Partition";
  }

  @Override
  public Storage<T> create(String name) throws IOException {
    boolean sync = false;
    if (properties.containsKey("Sync")) {
      sync = Boolean.parseBoolean(properties.get("Sync"));
    }
    int itemCount = ITEM_COUNT;
    if(properties.containsKey("ItemCount")){
      itemCount = Integer.parseInt(properties.get("ItemCount"));
    }
    long maxPartitionSize = MAXIMUM_DATA_SIZE;
    if(properties.containsKey("MaxPartitionSize")){
      maxPartitionSize = Long.parseLong(properties.get("MaxPartitionSize"));
    }

    int expiredEventPoll = EXPIRED_EVENT_MONITOR_TIME;
    if(properties.containsKey("ExpiredEventPoll")){
      expiredEventPoll = Integer.parseInt(properties.get("ExpiredEventPoll"));
    }

    if(expiredHandler == null){
      expiredHandler = new BaseExpiredHandler<>();
    }
    return new PartitionStorage<T>(name, storableFactory, expiredHandler, sync, itemCount, maxPartitionSize, expiredEventPoll);
  }

  @Override
  public List<Storage<T>> discovered() {
    return new ArrayList<>();
  }

}

