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
@Schema(description = "S3 configuration options")
public class S3Config {

  @Schema(description = "Enable S3 compression for uploaded data", defaultValue = "false")
  private boolean compression;

  @Schema(description = "S3 access key ID")
  private String accessKeyId;

  @Schema(description = "S3 secret access key")
  private String secretAccessKey;

  @Schema(description = "AWS region name")
  private String regionName;

  @Schema(description = "S3 bucket name")
  private String bucketName;


  public S3Config() {}

  public S3Config(S3Config lhs) {
    this.compression = lhs.compression;
    this.accessKeyId = lhs.accessKeyId;
    this.secretAccessKey = lhs.secretAccessKey;
    this.regionName = lhs.regionName;
    this.bucketName = lhs.bucketName;
  }

  public void fromMap(Map<String, String> properties) {
    accessKeyId = properties.get("S3AccessKeyId");
    secretAccessKey = properties.get("S3SecretAccessKey");
    regionName = properties.get("S3RegionName");
    bucketName = properties.get("S3BucketName");
    compression = Boolean.parseBoolean(properties.getOrDefault("S3CompressEnabled", "false"));
  }

  public void validate() {
    boolean s3FieldsUsed = accessKeyId != null || secretAccessKey != null || regionName != null || bucketName != null || compression;
    if (s3FieldsUsed) {
      if (isBlank(accessKeyId)) throw new IllegalArgumentException("S3 accessKeyId must be set");
      if (isBlank(secretAccessKey)) throw new IllegalArgumentException("S3 secretAccessKey must be set");
      if (isBlank(regionName)) throw new IllegalArgumentException("S3 regionName must be set");
      if (isBlank(bucketName)) throw new IllegalArgumentException("S3 bucketName must be set");
    }
  }

  private boolean isBlank(String s) {
    return s == null || s.isBlank();
  }
}
