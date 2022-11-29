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

package io.mapsmessaging.storage.impl.file.s3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SimpleS3Test {

  private int bufferSize = 2 * 1024 * 1024;

  private String accessKeyId;
  private String secretAccessKey;
  private String region;
  private String bucketName;

  @BeforeEach
  void loadS3Details(){
    accessKeyId = System.getProperty("accessKeyId");
    secretAccessKey = System.getProperty("secretAccessKey");
    region = System.getProperty("regionName");
    bucketName = System.getProperty("bucketName");
  }

  @Test
  void archiveAndDelete() throws IOException {
    Assumptions.assumeTrue(new ConfigurationCheck(), "AWS Configuration not preset");
    AmazonS3 amazonS3 = createAmazonId();
    S3TransferApi transferApi = new S3TransferApi(amazonS3, bucketName);
    String filename = "FileToArchive.dmp";
    FileOutputStream fileOutputStream = new FileOutputStream(filename, false);
    for(int x=0;x<1024 * 1024;x++){
      byte val = (byte)(x%0xff);
      fileOutputStream.write(val);
    }
    fileOutputStream.close();
    S3Record record = transferApi.archive("mapsMessaging/test", filename);
    Assertions.assertEquals(bucketName, record.getBucketName(), "Bucket names must match");
    Assertions.assertEquals(1024 * 1024, record.getLength(), "File Length should match");
    transferApi.delete(record);
    Assertions.assertThrowsExactly(IOException.class, () -> transferApi.retrieve(filename, record));
  }

  @Test
  void archiveAndRestore() throws IOException, NoSuchAlgorithmException {
    Assumptions.assumeTrue(new ConfigurationCheck(), "AWS Configuration not preset");
    AmazonS3 amazonS3 = createAmazonId();
    S3TransferApi transferApi = new S3TransferApi(amazonS3, bucketName);

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

    S3Record record = transferApi.archive("mapsMessaging/test", filename);

    Assertions.assertEquals(bucketName, record.getBucketName(), "Bucket names must match");
    Assertions.assertEquals(bufferSize, record.getLength(), "File Length should match");

    S3Record reloaded = new S3Record();
    reloaded.read(filename);
    Assertions.assertEquals(bucketName, reloaded.getBucketName(), "Bucket names must match");
    Assertions.assertEquals(bufferSize, reloaded.getLength(), "File Length should match");

    Assertions.assertEquals(record.getLength(), reloaded.getLength());
    Assertions.assertEquals(record.getEntryName(), reloaded.getEntryName());
    Assertions.assertEquals(record.getMd5(), reloaded.getMd5());
    Assertions.assertEquals(record.getBucketName(), reloaded.getBucketName());
    Assertions.assertEquals(record.getArchivedDate(), reloaded.getArchivedDate());

    transferApi.retrieve(filename, reloaded);
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

  private AmazonS3 createAmazonId(){
    AWSCredentials credentials = new BasicAWSCredentials(accessKeyId, secretAccessKey);
    Regions regions = Regions.fromName(region);
    return AmazonS3ClientBuilder.standard()
        .withRegion(regions)
        .withCredentials(new AWSStaticCredentialsProvider(credentials))
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
