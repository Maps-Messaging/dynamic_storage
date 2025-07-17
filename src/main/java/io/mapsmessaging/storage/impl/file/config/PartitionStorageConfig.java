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
package io.mapsmessaging.storage.impl.file.config;

import io.mapsmessaging.storage.StorableFactory;
import io.mapsmessaging.storage.StorageConfig;
import io.mapsmessaging.storage.impl.file.TaskQueue;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Map;

@EqualsAndHashCode(callSuper = false)
@Data
@Schema(description = "File-based partitioned storage configuration")
public class PartitionStorageConfig extends StorageConfig {

  private static final int ITEM_COUNT = 524_288;
  private static final long MAXIMUM_DATA_SIZE = 1L << 32; // 4GB
  private static final int EXPIRED_EVENT_MONITOR_TIME = 1;

  @Schema(description = "Storage file name (typically derived from the logical name)")
  private String fileName;

  @Schema(description = "Enable synchronous writes to disk", defaultValue = "false")
  private boolean sync;

  @Schema(description = "Number of items per partition", defaultValue = "524288")
  private int itemCount;

  @Schema(description = "Maximum number of events to hold", defaultValue = "-1")
  private int capacity;

  @Schema(description = "Maximum size of a single partition in bytes", defaultValue = "4294967296")
  private long maxPartitionSize;

  @Schema(description = "Polling interval (in seconds) for expired events", defaultValue = "1")
  private int expiredEventPoll;

  @Schema(description = "Task queue identifier used for expired message handling")
  private TaskQueue taskQueue;

  @Schema(description = "Configuration to manage paused/idle messages stores when timeouts occur ")
  private DeferredConfig deferredConfig;

  private transient StorableFactory storableFactory;

  public PartitionStorageConfig() {}

  public PartitionStorageConfig(PartitionStorageConfig lhs) {
    super(lhs);
    this.fileName = lhs.fileName;
    this.sync = lhs.sync;
    this.capacity = lhs.capacity;
    this.itemCount = lhs.itemCount;
    this.maxPartitionSize = lhs.maxPartitionSize;
    this.expiredEventPoll = lhs.expiredEventPoll;
    this.taskQueue = lhs.taskQueue;
    this.deferredConfig = new DeferredConfig(lhs.deferredConfig);
    this.storableFactory = lhs.storableFactory;
  }

  @Override
  public void fromMap(String name, Map<String, String> properties) {
    super.fromMap(name, properties);
    fileName = name;
    sync = Boolean.parseBoolean(properties.getOrDefault("Sync", "false"));
    itemCount = Integer.parseInt(properties.getOrDefault("ItemCount", String.valueOf(ITEM_COUNT)));
    capacity = Integer.parseInt(properties.getOrDefault("Capacity", "-1"));
    maxPartitionSize = Long.parseLong(properties.getOrDefault("MaxPartitionSize", String.valueOf(MAXIMUM_DATA_SIZE)));
    expiredEventPoll = Integer.parseInt(properties.getOrDefault("ExpiredEventPoll", String.valueOf(EXPIRED_EVENT_MONITOR_TIME)));
    deferredConfig = new DeferredConfig();
    deferredConfig.fromMap(name, properties);
    setTaskQueue(taskQueue); // Note: taskQueue remains null unless set elsewhere

  }
}
