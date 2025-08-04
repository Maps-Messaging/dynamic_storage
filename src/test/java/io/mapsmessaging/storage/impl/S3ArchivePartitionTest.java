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

package io.mapsmessaging.storage.impl;

import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.storage.TierMigrationMonitor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

class S3ArchivePartitionTest extends BaseTest {

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void s3ArchiveAndRestoreCompressDigestPartition(boolean compress) throws IOException, InterruptedException {
    doTest(compress, "MD5");
  }

  @ParameterizedTest
  @ValueSource(strings = {"None", "MD5", "SHA-1", "SHA-256"})
  void s3ArchiveAndRestoreDigestDigestPartition(String digest) throws IOException, InterruptedException {
    doTest(true, digest);
  }

  void doTest(boolean compress, String digest) throws IOException, InterruptedException {
    Properties envProperties = S3Helper.getProperties();
    String accessKeyId = envProperties.getProperty("accessKeyId");
    String secretAccessKey = envProperties.getProperty("secretAccessKey");
    String region = envProperties.getProperty("regionName");
    String bucketName = envProperties.getProperty("bucketName");

    if(accessKeyId != null && secretAccessKey != null && region != null && bucketName != null) {
      S3Client amazonS3 = createS3Client(accessKeyId, secretAccessKey, region);
      clearBucket(amazonS3, bucketName);

      Assertions.assertTrue(isBucketEmpty(amazonS3, bucketName), "S3 bucket should be empty before the test starts");
      Map<String, String> properties = BasePartitionStoreTest.buildProperties(false);
      properties.put("deferredName", "S3");
      properties.put("archiveIdleTime", ""+ TimeUnit.SECONDS.toMillis(4));
      properties.put("S3AccessKeyId", accessKeyId);
      properties.put("S3SecretAccessKey",secretAccessKey);
      properties.put("S3RegionName", region);
      properties.put("S3BucketName", bucketName);
      properties.put("S3CompressEnabled", ""+compress);
      properties.put("digestName", digest);
      Storage<MappedData> storage = BasePartitionStoreTest.build(properties, testName);
      for (int x = 0; x < 1100; x++) {
        MappedData message = createMessageBuilder(x);
        storage.add(message);
      }

      // We should have exceeded the partition limits and have 10 partitions, lets wait the time out period
      TimeUnit.SECONDS.sleep(5);
      ((TierMigrationMonitor)storage).scanForArchiveMigration();
      Assertions.assertEquals(10, getBucketEntityCount(amazonS3, bucketName), "S3 bucket should have ten entries");

      // They should now be archived
      for (int x = 0; x < 1100; x++) {
        MappedData data = storage.get(x);
        Assertions.assertNotNull(data, "Expected data for key "+x);
        Assertions.assertEquals(data.key, x);

      }
      Assertions.assertTrue(isBucketEmpty(amazonS3, bucketName), "S3 bucket should be empty when the test finishes");
      storage.delete();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void s3ArchiveAndDeleteStore(boolean compress) throws IOException, InterruptedException {
    Properties envProperties = S3Helper.getProperties();

    String accessKeyId = envProperties.getProperty("accessKeyId");
    String secretAccessKey = envProperties.getProperty("secretAccessKey");
    String region = envProperties.getProperty("regionName");
    String bucketName = envProperties.getProperty("bucketName");
    if(accessKeyId != null && secretAccessKey != null && region != null && bucketName != null) {
      S3Client amazonS3 = createS3Client(accessKeyId, secretAccessKey, region);

      Assertions.assertTrue(isBucketEmpty(amazonS3, bucketName), "S3 bucket should be empty before the test starts");
      Map<String, String> properties = BasePartitionStoreTest.buildProperties(false);
      properties.put("deferredName", "S3");
      properties.put("archiveIdleTime", ""+TimeUnit.SECONDS.toMillis(4));
      properties.put("S3AccessKeyId", accessKeyId);
      properties.put("S3SecretAccessKey",secretAccessKey);
      properties.put("S3RegionName", region);
      properties.put("S3BucketName", bucketName);
      properties.put("S3CompressEnabled", ""+compress);
      Storage<MappedData> storage = BasePartitionStoreTest.build(properties, testName);
      for (int x = 0; x < 1100; x++) {
        MappedData message = createMessageBuilder(x);
        storage.add(message);
      }

      // We should have exceeded the partition limits and have 10 partitions, lets wait the time out period
      TimeUnit.SECONDS.sleep(5);
      ((TierMigrationMonitor)storage).scanForArchiveMigration();
      Assertions.assertEquals(10, getBucketEntityCount(amazonS3, bucketName), "S3 bucket should have ten entries");
      storage.delete();
      Assertions.assertTrue(isBucketEmpty(amazonS3, bucketName), "S3 bucket should be empty when the test finishes");
    }
  }

  private static S3Client createS3Client(String accessKeyId, String secretAccessKey, String region) {
    AwsBasicCredentials credentials = AwsBasicCredentials.create(accessKeyId, secretAccessKey);
    return S3Client.builder()
        .region(Region.of(region))
        .credentialsProvider(StaticCredentialsProvider.create(credentials))
        .build();
  }

  static boolean isBucketEmpty(S3Client s3Client, String bucketName) {
    return getBucketEntityCount(s3Client, bucketName) == 0;
  }

  static void clearBucket(S3Client s3Client, String bucketName) {
    ListObjectsV2Response response = s3Client.listObjectsV2(
        ListObjectsV2Request.builder().bucket(bucketName).build()
    );

    for (S3Object s3Object : response.contents()) {
      s3Client.deleteObject(DeleteObjectRequest.builder()
          .bucket(bucketName)
          .key(s3Object.key())
          .build());
    }
  }

  static int getBucketEntityCount(S3Client s3Client, String bucketName) {
    ListObjectsV2Request request = ListObjectsV2Request.builder()
        .bucket(bucketName)
        .maxKeys(1000) // more efficient when only checking if empty
        .build();
    ListObjectsV2Response response = s3Client.listObjectsV2(request);
    for(S3Object s3Object: response.contents()){
      System.err.println(s3Object.key());
    }
    return response.keyCount();
  }

}
