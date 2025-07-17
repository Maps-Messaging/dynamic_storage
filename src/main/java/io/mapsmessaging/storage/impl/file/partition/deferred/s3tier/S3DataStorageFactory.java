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

package io.mapsmessaging.storage.impl.file.partition.deferred.s3tier;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.impl.file.config.PartitionStorageConfig;
import io.mapsmessaging.storage.impl.file.config.S3Config;
import io.mapsmessaging.storage.impl.file.partition.DataStorageFactory;
import io.mapsmessaging.storage.impl.file.partition.DeferredDataStorage;

import java.io.IOException;

public class S3DataStorageFactory<T extends Storable> implements DataStorageFactory<T> {

  public S3DataStorageFactory(){
    // Only needed for service loading
  }

  @Override
  public String getName() {
    return "S3";
  }

  @Override
  public DeferredDataStorage<T> create(PartitionStorageConfig config)
      throws IOException {

    S3Config s3Config = config.getDeferredConfig().getS3Config();
    AWSCredentials credentials = new BasicAWSCredentials(
        s3Config.getAccessKeyId(),
        s3Config.getSecretAccessKey()
    );
    Regions regions = fromRegionName(s3Config.getRegionName());
    AmazonS3 amazonS3 = AmazonS3ClientBuilder.standard()
        .withRegion(regions)
        .withCredentials(new AWSStaticCredentialsProvider(credentials))
        .build();
    S3TransferApi transferApi = new S3TransferApi(amazonS3, s3Config.getBucketName(), s3Config.isCompression());
    return new S3DataStorageProxy<>(transferApi, config);
  }

  private Regions fromRegionName(String regionName) {
    return java.util.Arrays.stream(Regions.values())
        .filter(region -> region.getName().equals(regionName))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("Unknown region: " + regionName));
  }
}
