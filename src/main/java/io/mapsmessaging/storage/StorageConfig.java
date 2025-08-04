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

package io.mapsmessaging.storage;


import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.mapsmessaging.storage.impl.file.config.PartitionStorageConfig;
import io.mapsmessaging.storage.impl.memory.MemoryStorageConfig;
import io.mapsmessaging.storage.impl.tier.memory.MemoryTierConfig;
import io.swagger.v3.oas.annotations.media.DiscriminatorMapping;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@SuppressWarnings("javaarchitecture:S7091")
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type"
)
@JsonSubTypes({
    @JsonSubTypes.Type(value = MemoryStorageConfig.class, name = "memory"),
    @JsonSubTypes.Type(value = PartitionStorageConfig.class, name = "partition"),
    @JsonSubTypes.Type(value = MemoryTierConfig.class, name = "tieredMemory")
})
@Schema(
    description = "Base class for all storage configurations",
    discriminatorProperty = "type",
    oneOf = { MemoryStorageConfig.class, PartitionStorageConfig.class, MemoryTierConfig.class },
    discriminatorMapping = {
        @DiscriminatorMapping(value = "memory", schema = MemoryStorageConfig.class),
        @DiscriminatorMapping(value = "partition", schema = PartitionStorageConfig.class),
        @DiscriminatorMapping(value = "tieredMemory", schema = MemoryTierConfig.class)
    }
)

@Getter
@Setter
public class StorageConfig {

  protected String type;

  @Schema(description = "Enable debug logging for this storage component", defaultValue = "false")
  private boolean debug;

  public StorageConfig() {}

  public StorageConfig(StorageConfig lhs){
    debug = lhs.debug;
    type = lhs.type;
  }

  public void fromMap(Map<String, String> properties){
    debug = properties.containsKey("debug") && Boolean.parseBoolean(properties.get("debug"));
  }

  public StorageConfig getCopy(){
    return new StorageConfig(this);
  }

}
