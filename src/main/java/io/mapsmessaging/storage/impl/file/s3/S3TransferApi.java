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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import java.io.File;
import java.io.IOException;
import java.util.UUID;

public class S3TransferApi {

  private final AmazonS3 amazonS3;
  private final String bucketName;

  public S3TransferApi(AmazonS3 amazonS3, String bucketName){
    this.amazonS3 = amazonS3;
    this.bucketName = bucketName;
  }

  public void delete(S3Record s3Record ){
    amazonS3.deleteObject(s3Record.getBucketName(), s3Record.getEntryName());
  }

  public boolean retrieve(String localFileName, S3Record s3Record) throws IOException {
    File file = new File(localFileName);
    if(!file.delete()){
      System.err.println("Log this fact");
    }
    GetObjectRequest getObjectRequest = new GetObjectRequest(s3Record.getBucketName(), s3Record.getEntryName());
    ObjectMetadata objectMetadata = amazonS3.getObject(getObjectRequest, file);
    boolean success = s3Record.getMd5().equals(objectMetadata.getContentMD5());
    if(!success) {
      throw new IOException("MD5 signatures do not match");
    }
    amazonS3.deleteObject(s3Record.getBucketName(), s3Record.getEntryName());
    return success;
  }

  public S3Record archive(String path, String localFileName) throws IOException {
    File file = new File(localFileName);
    String entryName = path+"/"+UUID.randomUUID();
    PutObjectResult putObjectResult = amazonS3.putObject(bucketName, entryName, file);
    S3Record s3Record = new S3Record(bucketName, entryName,  putObjectResult.getContentMd5(), file.length());
    s3Record.write(localFileName);
    return s3Record;
  }
}
