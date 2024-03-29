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

package io.mapsmessaging.storage.impl.file.partition.archive.s3tier;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.impl.file.PartitionStorageConfig;
import io.mapsmessaging.storage.impl.file.partition.ArchivedDataStorage;
import io.mapsmessaging.storage.impl.file.partition.DataStorageFactory;
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
  public ArchivedDataStorage<T> create(PartitionStorageConfig<T> config)
      throws IOException {
    AWSCredentials credentials = new BasicAWSCredentials(
        config.getS3AccessKeyId(),
        config.getS3SecretAccessKey()
    );
    Regions regions = Regions.fromName(config.getS3RegionName());
    AmazonS3 amazonS3 = AmazonS3ClientBuilder.standard()
        .withRegion(regions)
        .withCredentials(new AWSStaticCredentialsProvider(credentials))
        .build();
    S3TransferApi transferApi = new S3TransferApi(amazonS3, config.getS3BucketName(), config.isS3Compression());
    return new S3DataStorageProxy<>(transferApi, config);
  }
}
