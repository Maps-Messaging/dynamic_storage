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

import static io.mapsmessaging.storage.logging.StorageLogMessages.S3_ARCHIVING_DATA;
import static io.mapsmessaging.storage.logging.StorageLogMessages.S3_ENTITY_DELETED;
import static io.mapsmessaging.storage.logging.StorageLogMessages.S3_FILE_DELETE_FAILED;
import static io.mapsmessaging.storage.logging.StorageLogMessages.S3_MD5_HASH_FAILED;
import static io.mapsmessaging.storage.logging.StorageLogMessages.S3_RESTORED_DATA;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import io.mapsmessaging.logging.Logger;
import io.mapsmessaging.logging.LoggerFactory;
import io.mapsmessaging.storage.impl.file.partition.archive.StreamProcessor;
import io.mapsmessaging.storage.impl.file.partition.archive.compress.FileCompressionProcessor;
import io.mapsmessaging.storage.impl.file.partition.archive.compress.StreamCompressionHelper;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

public class S3TransferApi {
  private static final Logger LOGGER = LoggerFactory.getLogger(S3TransferApi.class);

  private final AmazonS3 amazonS3;
  private final String bucketName;
  private final boolean compress;

  public S3TransferApi(AmazonS3 amazonS3, String bucketName, boolean compress){
    this.amazonS3 = amazonS3;
    this.bucketName = bucketName;
    this.compress = compress;
  }

  public void delete(S3Record s3Record ){
    amazonS3.deleteObject(s3Record.getBucketName(), s3Record.getEntryName());
    LOGGER.log(S3_ENTITY_DELETED, s3Record.getEntryName());
  }

  public void retrieve(String localFileName, S3Record s3Record) throws IOException {
    File file = new File(localFileName);
    if(!file.delete()){
      LOGGER.log(S3_FILE_DELETE_FAILED, localFileName);
      throw new IOException("Unable to delete placeholder file");
    }
    boolean success;
    try {
      S3Object s3Object = amazonS3.getObject(s3Record.getBucketName(), s3Record.getEntryName());
      MessageDigest messageDigest = MessageDigest.getInstance("MD5");
      StreamProcessor streamProcessor;
      if(s3Record.isCompressed()) {
        streamProcessor = new StreamCompressionHelper();
      }
      else{
        streamProcessor = new StreamProcessor();
      }

      try(FileOutputStream fileOutputStream = new FileOutputStream(file, false)) {
        streamProcessor.out(s3Object.getObjectContent(), fileOutputStream, messageDigest);
      }
      byte[] md5Hash = messageDigest.digest();
      String base = Base64.getEncoder().encodeToString(md5Hash);
      success = base.equals(s3Record.getMd5());
      if(!success) {
        LOGGER.log(S3_MD5_HASH_FAILED, localFileName, s3Record.getMd5(), base);
        throw new IOException("MD5 mismatch Computed:"+base+" original "+s3Record.getMd5());
      }
      amazonS3.deleteObject(s3Record.getBucketName(), s3Record.getEntryName());
      LOGGER.log(S3_RESTORED_DATA, localFileName, bucketName);
    } catch (NoSuchAlgorithmException | AmazonS3Exception | DigestException e) {
      throw new IOException(e);
    }
  }

  public S3Record archive(String path, String localFileName) throws IOException {
    File file = new File(localFileName);
    String digest = null;
    if(compress) {
      try {
        MessageDigest messageDigest = MessageDigest.getInstance("MD5");
        File zip = new File(localFileName + "_zip");
        FileCompressionProcessor fileCompressionProcessor = new FileCompressionProcessor();
        fileCompressionProcessor.in(file, zip, messageDigest);
        file = zip;
        digest = Base64.getEncoder().encodeToString(messageDigest.digest());
      } catch (NoSuchAlgorithmException e) {
        throw new IOException(e);
      }
    }
    String entryName = path+"/"+file.getName();
    PutObjectResult putObjectResult = amazonS3.putObject(bucketName, entryName, file);
    if(digest == null){
      digest = putObjectResult.getContentMd5();
    }
    S3Record s3Record = new S3Record(bucketName, entryName,  digest, file.length(), compress);
    file.delete(); // Removes the _zip file
    s3Record.write(localFileName);
    LOGGER.log(S3_ARCHIVING_DATA, localFileName, bucketName);
    return s3Record;
  }
}
