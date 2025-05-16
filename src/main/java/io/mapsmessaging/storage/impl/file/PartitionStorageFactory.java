/*
 *
 *  Copyright [ 2020 - 2024 ] Matthew Buckton
 *  Copyright [ 2024 - 2025 ] MapsMessaging B.V.
 *
 *  Licensed under the Apache License, Version 2.0 with the Commons Clause
 *  (the "License"); you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at:
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *      https://commonsclause.com/
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.mapsmessaging.storage.impl.file;

import io.mapsmessaging.storage.*;
import io.mapsmessaging.storage.impl.BaseStorageFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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

  public PartitionStorageFactory(Map<String, String> properties, StorableFactory<T> storableFactory, ExpiredStorableHandler expiredHandler) {
    super(properties, storableFactory, expiredHandler);
  }

  @Override
  public String getName() {
    return "Partition";
  }

  @Override
  public StorageFactory<T> getInstance(@NotNull Map<String, String> properties, @NotNull StorableFactory<T> storableFactory, @Nullable ExpiredStorableHandler expiredHandler) {
    return new PartitionStorageFactory<>(properties, storableFactory, expiredHandler);
  }

  @Override
  public Storage<T> create(String name) throws IOException {
    return create(name, new TaskQueue());
  }

  public Storage<T> create(String name, TaskQueue taskQueue) throws IOException {
    PartitionStorageConfig<T> config = new PartitionStorageConfig<>();
    boolean sync = false;
    if (properties.containsKey("Sync")) {
      sync = Boolean.parseBoolean(properties.get("Sync"));
    }
    config.setSync(sync);

    int itemCount = ITEM_COUNT;
    if (properties.containsKey("ItemCount")) {
      itemCount = Integer.parseInt(properties.get("ItemCount"));
    }
    config.setItemCount(itemCount);

    long maxPartitionSize = MAXIMUM_DATA_SIZE;
    if (properties.containsKey("MaxPartitionSize")) {
      maxPartitionSize = Long.parseLong(properties.get("MaxPartitionSize"));
    }
    config.setMaxPartitionSize(maxPartitionSize);

    config.setFileName(name);
    config.setTaskQueue(taskQueue);

    int expiredEventPoll = EXPIRED_EVENT_MONITOR_TIME;
    if (properties.containsKey("ExpiredEventPoll")) {
      expiredEventPoll = Integer.parseInt(properties.get("ExpiredEventPoll"));
    }
    config.setExpiredEventPoll(expiredEventPoll);
    config.setStorableFactory(storableFactory);
    config.setExpiredHandler(expiredHandler);

    if(properties.containsKey("archiveName")) {
      config.setArchiveName(properties.get("archiveName"));
    }
    config.setMigrationDestination(properties.get("migrationPath"));

    config.setS3AccessKeyId(properties.get("S3AccessKeyId"));
    config.setS3SecretAccessKey(properties.get("S3SecretAccessKey"));
    config.setS3RegionName(properties.get("S3RegionName"));
    config.setS3BucketName(properties.get("S3BucketName"));
    config.setS3Compression(Boolean.parseBoolean(properties.get("S3CompressEnabled")));
    if(properties.containsKey("archiveIdleTime")) {
      config.setArchiveIdleTime(Integer.parseInt(properties.get("archiveIdleTime")));
    }
    if(properties.containsKey("digestName")) {
      config.setDigestName(properties.get("digestName"));
    }
    return new PartitionStorage<>(config);
  }

  @Override
  public List<Storage<T>> discovered() {
    return new ArrayList<>();
  }

}

