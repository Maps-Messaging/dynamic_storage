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

package io.mapsmessaging.storage.impl.file.s3;

import io.mapsmessaging.storage.impl.S3Helper;
import io.mapsmessaging.storage.impl.file.partition.deferred.s3tier.S3Record;
import io.mapsmessaging.storage.impl.file.partition.deferred.s3tier.S3TransferApi;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;
import java.util.function.BooleanSupplier;

class SimpleS3Test {

  private int bufferSize = 2 * 1024 * 1024;

  private String accessKeyId;
  private String secretAccessKey;
  private String region;
  private String bucketName;

  @BeforeEach
  void loadS3Details() throws IOException {
    Properties envProperties = S3Helper.getProperties();
    accessKeyId = envProperties.getProperty("accessKeyId");
    secretAccessKey = envProperties.getProperty("secretAccessKey");
    region = envProperties.getProperty("regionName");
    bucketName = envProperties.getProperty("bucketName");
  }

  @Test
  void archiveAndDelete() throws IOException {
    Assumptions.assumeTrue(new ConfigurationCheck(), "AWS Configuration not preset");
    S3Client amazonS3 = createAmazonId();
    S3TransferApi transferApi = new S3TransferApi(amazonS3, bucketName, false);
    String filename = "FileToArchive.dmp";
    FileOutputStream fileOutputStream = new FileOutputStream(filename, false);
    for(int x=0;x<1024 * 1024;x++){
      byte val = (byte)(x%0xff);
      fileOutputStream.write(val);
    }
    fileOutputStream.close();
    S3Record record = transferApi.archive("mapsMessaging/test", filename, null);
    Assertions.assertEquals(bucketName, record.getBucketName(), "Bucket names must match");
    Assertions.assertEquals(1024 * 1024, record.getLength(), "File Length should match");
    transferApi.delete(record);
    Assertions.assertThrowsExactly(IOException.class, () -> transferApi.retrieve(filename, record, null));
  }

  @Test
  void archiveAndRestore() throws IOException, NoSuchAlgorithmException {
    Assumptions.assumeTrue(new ConfigurationCheck(), "AWS Configuration not preset");
    S3Client amazonS3 = createAmazonId();
    S3TransferApi transferApi = new S3TransferApi(amazonS3, bucketName, false);

    String filename = "FileToArchive.dmp";
    MessageDigest md = MessageDigest.getInstance("SHA-256");
    FileOutputStream fileOutputStream = new FileOutputStream(filename, false);
    byte[] buf = new byte[256];
    for(int x=0;x<buf.length;x++){
      byte val = (byte)(x%0xff);
      buf[x] = val;
    }


    for(int x=0;x<bufferSize/buf.length;x++){
      fileOutputStream.write(buf);
      md.update(buf);
    }
    byte[] digest = md.digest();
    fileOutputStream.close();

    S3Record record = transferApi.archive("mapsMessaging/test", filename, null);
    record.write(filename);

    Assertions.assertEquals(bucketName, record.getBucketName(), "Bucket names must match");
    Assertions.assertEquals(bufferSize, record.getLength(), "File Length should match");

    S3Record reloaded = new S3Record();
    reloaded.read(filename);
    Assertions.assertEquals(bucketName, reloaded.getBucketName(), "Bucket names must match");
    Assertions.assertEquals(bufferSize, reloaded.getLength(), "File Length should match");

    Assertions.assertEquals(record.getLength(), reloaded.getLength());
    Assertions.assertEquals(record.getEntryName(), reloaded.getEntryName());
    Assertions.assertEquals(record.getDeferredHash(), reloaded.getDeferredHash());
    Assertions.assertEquals(record.getBucketName(), reloaded.getBucketName());
    Assertions.assertEquals(record.getArchivedDate(), reloaded.getArchivedDate());

    transferApi.retrieve(filename, reloaded, null);
    FileInputStream fileInputStream = new FileInputStream(filename);
    byte[] tmp = new byte[1024];
    int read = 1;
    md.reset();
    while(read >0){
      read = fileInputStream.read(tmp, 0, tmp.length);
      if(read >0) {
        md.update(tmp, 0, read);
      }
    }
    byte[] reloadedDigest = md.digest();
    Assertions.assertArrayEquals(digest, reloadedDigest);
  }

  private S3Client createAmazonId(){
    AwsBasicCredentials credentials = AwsBasicCredentials.create(accessKeyId, secretAccessKey);
    return S3Client.builder()
        .region(Region.of(region))
        .credentialsProvider(StaticCredentialsProvider.create(credentials))
        .build();
  }


  private boolean isConfigured(){
    return accessKeyId != null && secretAccessKey != null && region != null && bucketName != null;
  }

  class ConfigurationCheck implements BooleanSupplier {
    @Override
    public boolean getAsBoolean() {
      // check the endpoint here and return either true or false
      return isConfigured();
    }
  }
}
