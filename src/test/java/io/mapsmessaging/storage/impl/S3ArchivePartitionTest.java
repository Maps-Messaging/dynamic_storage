package io.mapsmessaging.storage.impl;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.storage.impl.file.PartitionStorage;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class S3ArchivePartitionTest extends BaseTest {

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void s3ArchiveAndRestorePartition(boolean compress) throws IOException, InterruptedException {
    String accessKeyId = System.getProperty("accessKeyId");
    String secretAccessKey = System.getProperty("secretAccessKey");
    String region = System.getProperty("regionName");
    String bucketName = System.getProperty("bucketName");

    if(accessKeyId != null && secretAccessKey != null && region != null && bucketName != null) {
      AmazonS3 amazonS3 = createAmazonId(accessKeyId, secretAccessKey, region);

      Assertions.assertTrue(isBucketEmpty(amazonS3, bucketName), "S3 bucket should be empty before the test starts");
      Map<String, String> properties = BasePartitionStoreTest.buildProperties(false);
      properties.put("archiveName", "S3");
      properties.put("archiveIdleTime", ""+ TimeUnit.SECONDS.toMillis(30));
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
      TimeUnit.SECONDS.sleep(40);
      ((PartitionStorage<MappedData>)storage).scanForArchiveMigration();
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
    String accessKeyId = System.getProperty("accessKeyId");
    String secretAccessKey = System.getProperty("secretAccessKey");
    String region = System.getProperty("regionName");
    String bucketName = System.getProperty("bucketName");
    if(accessKeyId != null && secretAccessKey != null && region != null && bucketName != null) {
      AmazonS3 amazonS3 = createAmazonId(accessKeyId, secretAccessKey, region);

      Assertions.assertTrue(isBucketEmpty(amazonS3, bucketName), "S3 bucket should be empty before the test starts");
      Map<String, String> properties = BasePartitionStoreTest.buildProperties(false);
      properties.put("archiveName", "S3");
      properties.put("archiveIdleTime", ""+TimeUnit.SECONDS.toMillis(30));
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
      TimeUnit.SECONDS.sleep(40);
      ((PartitionStorage<MappedData>)storage).scanForArchiveMigration();
      Assertions.assertEquals(10, getBucketEntityCount(amazonS3, bucketName), "S3 bucket should have ten entries");
      storage.delete();
      Assertions.assertTrue(isBucketEmpty(amazonS3, bucketName), "S3 bucket should be empty when the test finishes");
    }
  }

  private AmazonS3 createAmazonId(String accessKeyId, String secretAccessKey, String region){

    AWSCredentials credentials = new BasicAWSCredentials(accessKeyId, secretAccessKey);
    Regions regions = Regions.fromName(region);
    return AmazonS3ClientBuilder.standard()
        .withRegion(regions)
        .withCredentials(new AWSStaticCredentialsProvider(credentials))
        .build();
  }

  boolean isBucketEmpty(AmazonS3 amazonS3, String bucketName){
    return getBucketEntityCount(amazonS3, bucketName) == 0;
  }

  int getBucketEntityCount(AmazonS3 amazonS3, String bucketName){
    ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName);
    ListObjectsV2Result listing = amazonS3.listObjectsV2(req);
    return listing.getKeyCount();
  }

}
