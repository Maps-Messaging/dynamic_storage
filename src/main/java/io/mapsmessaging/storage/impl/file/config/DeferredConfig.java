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

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

import java.util.Map;

@Data
public class DeferredConfig {

  @Schema(
      description = "Archive strategy for rotated partitions: - **None**: No deferral; data remains in place. - **S3**: Move partitions to S3-compatible storage. - **Compress**: Compress data locally on disk. - **Migrate**: Move data to another (typically slower or network-based) store.",
      defaultValue = "None"
  )
  private String deferredName = "None";

  @Schema(description = "Time in milliseconds after which data should be archived", defaultValue = "-1")
  private long idleTime = -1;

  @Schema(description = "Destination directory or location for data migration")
  private String migrationDestination;

  @Schema(description = "Digest algorithm name for checksums (e.g., SHA-256)")
  private String digestName = "";

  @Schema(description = "Optional S3 configuration parameters to use to push stale partitions to")
  private S3Config s3Config;


  public DeferredConfig(){}

  public DeferredConfig(DeferredConfig lhs) {
    this.deferredName = lhs.deferredName;
    this.idleTime = lhs.idleTime;
    this.migrationDestination = lhs.migrationDestination;
    this.digestName = lhs.digestName;

    if(lhs.s3Config != null){
      s3Config = new S3Config(lhs.s3Config);
    }
    else {
      this.s3Config = null;
    }
  }

  public void fromMap(Map<String, String> properties) {

    if (properties.containsKey("deferredName")) {
      deferredName = properties.get("deferredName");
    }
    if (properties.containsKey("archiveIdleTime")) {
      idleTime = Long.parseLong(properties.get("archiveIdleTime"));
    }
    if (properties.containsKey("digestName")) {
      digestName = properties.get("digestName");
    }

    migrationDestination = properties.get("migrationPath");

    if(s3Config == null) {
      s3Config = new S3Config();
    }
    s3Config.fromMap(properties);
  }
}
