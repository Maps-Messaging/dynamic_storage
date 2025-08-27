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
package io.mapsmessaging.storage.impl.tier.memory;

import io.mapsmessaging.storage.StorageConfig;
import io.mapsmessaging.storage.impl.file.config.PartitionStorageConfig;
import io.mapsmessaging.storage.impl.memory.MemoryStorageConfig;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Map;

@EqualsAndHashCode(callSuper = false)
@Data
@Schema(description = "Tiered memory storage configuration combining in-memory and partitioned storage")
public class MemoryTierConfig extends StorageConfig {

  private static final long DEFAULT_MIGRATION_TIME = 60_000;
  private static final long DEFAULT_SCAN_INTERVAL = 10_000;
  private static final long DEFAULT_TIER_1_SIZE = 0;

  @Schema(description = "Time in milliseconds after which data is migrated from memory to disk", defaultValue = "60000")
  private long migrationTime;

  @Schema(description = "Interval in milliseconds to scan for data eligible for migration", defaultValue = "10000")
  private long scanInterval;

  @Schema(description = "Maximum number of events to retain in memory before migration", defaultValue = "0")
  private long maximumCount;

  @Schema(description = "Configuration for the in-memory tier")
  private MemoryStorageConfig memoryStorageConfig;

  @Schema(description = "Configuration for the disk-backed partition tier")
  private PartitionStorageConfig partitionStorageConfig;

  public MemoryTierConfig() {
    type = "MemoryTier";

  }

  public MemoryTierConfig(MemoryTierConfig lhs) {
    super(lhs);
    type = "MemoryTier";
    this.migrationTime = lhs.migrationTime;
    this.scanInterval = lhs.scanInterval;
    this.maximumCount = lhs.maximumCount;
    this.memoryStorageConfig = new MemoryStorageConfig(lhs.memoryStorageConfig);
    this.partitionStorageConfig = new PartitionStorageConfig(lhs.partitionStorageConfig);
  }

  @Override
  public StorageConfig getCopy(){
    return new MemoryTierConfig(this);
  }

  @Override
  public void fromMap(Map<String, String> properties) {
    super.fromMap(properties);

    migrationTime = Long.parseLong(properties.getOrDefault("MigrationPeriod", String.valueOf(DEFAULT_MIGRATION_TIME)));
    scanInterval = Long.parseLong(properties.getOrDefault("ScanInterval", String.valueOf(DEFAULT_SCAN_INTERVAL)));
    maximumCount = Long.parseLong(properties.getOrDefault("Tier1Size", String.valueOf(DEFAULT_TIER_1_SIZE)));

    memoryStorageConfig = new MemoryStorageConfig();
    memoryStorageConfig.fromMap(properties);

    partitionStorageConfig = new PartitionStorageConfig();
    partitionStorageConfig.fromMap(properties);
  }
}
