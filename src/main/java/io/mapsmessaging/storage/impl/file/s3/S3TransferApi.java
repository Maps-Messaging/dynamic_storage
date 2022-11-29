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
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

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

  public void retrieve(String localFileName, S3Record s3Record) throws IOException {
    File file = new File(localFileName);
    boolean success;
    try {
      S3Object s3Object = amazonS3.getObject(s3Record.getBucketName(), s3Record.getEntryName());
      if(!file.delete()){
        System.err.println("Log this fact");
      }
      MessageDigest messageDigest = MessageDigest.getInstance("MD5");
      try(FileOutputStream fileOutputStream = new FileOutputStream(file, false)){
        long read = 0;
        byte[] tmp = new byte[1024];
        S3ObjectInputStream inputStream = s3Object.getObjectContent();
        while(read < s3Record.getLength()){
          int t  = inputStream.read(tmp, 0, tmp.length);
          if(t > 0) {
            messageDigest.update(tmp, 0, t);
            fileOutputStream.write(tmp, 0, t);
            read += t;
          }
        }
      }
      byte[] md5Hash = messageDigest.digest();
      String base = Base64.getEncoder().encodeToString(md5Hash);
      success = base.equals(s3Record.getMd5());
      if(!success) {
        throw new IOException("MD5 mismatch");
      }
      amazonS3.deleteObject(s3Record.getBucketName(), s3Record.getEntryName());
    } catch (NoSuchAlgorithmException | AmazonS3Exception e) {
      throw new IOException(e);
    }
  }

  public S3Record archive(String path, String localFileName) throws IOException {
    File file = new File(localFileName);
    String entryName = path+"/"+file.getName();
    PutObjectResult putObjectResult = amazonS3.putObject(bucketName, entryName, file);
    S3Record s3Record = new S3Record(bucketName, entryName,  putObjectResult.getContentMd5(), file.length());
    s3Record.write(localFileName);
    return s3Record;
  }
}
