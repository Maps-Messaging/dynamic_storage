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
package io.mapsmessaging.storage.impl.memory;

import io.mapsmessaging.storage.StorageConfig;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Map;

@EqualsAndHashCode(callSuper = false)
@Data
@Schema(
    name = "MemoryStorageConfig",
    description = "In-memory storage configuration"
)
public class MemoryStorageConfig extends StorageConfig {

  private static final int EXPIRED_EVENT_MONITOR_TIME = 1;

  @Schema(description = "Polling interval (in seconds) for checking expired events", example = "1")
  private int expiredEventPoll;

  @Schema(description = "Maximum number of messages that can be held in memory", example = "10000", defaultValue = "-1")
  private int capacity;

  public MemoryStorageConfig() {}

  public MemoryStorageConfig(MemoryStorageConfig lhs){
    super(lhs);
    expiredEventPoll = lhs.expiredEventPoll;
    capacity = lhs.capacity;
  }

  @Override
  public void fromMap(String name, Map<String, String> properties){
    super.fromMap(name, properties);
    expiredEventPoll = Integer.parseInt(properties.getOrDefault("ExpiredEventPoll", String.valueOf(EXPIRED_EVENT_MONITOR_TIME)));
    capacity = Integer.parseInt(properties.getOrDefault("Capacity", "-1"));
  }
}

