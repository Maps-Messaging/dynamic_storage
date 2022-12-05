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
import com.amazonaws.services.s3.model.S3Object;
import io.mapsmessaging.logging.Logger;
import io.mapsmessaging.logging.LoggerFactory;
import io.mapsmessaging.storage.impl.file.FileHelper;
import io.mapsmessaging.storage.impl.file.partition.archive.StreamProcessor;
import io.mapsmessaging.storage.impl.file.partition.archive.compress.FileCompressionProcessor;
import io.mapsmessaging.storage.impl.file.partition.archive.compress.StreamCompressionHelper;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
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

  public void retrieve(String localFileName, S3Record s3Record, MessageDigest messageDigest) throws IOException {
    File file = new File(localFileName);
    if(!FileHelper.delete(localFileName)){
      LOGGER.log(S3_FILE_DELETE_FAILED, localFileName);
      throw new IOException("Unable to delete placeholder file");
    }
    try {
      S3Object s3Object = amazonS3.getObject(s3Record.getBucketName(), s3Record.getEntryName());
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
      if(messageDigest != null) {
        byte[] hash = messageDigest.digest();
        String base = Base64.getEncoder().encodeToString(hash);
        if(!base.equals(s3Record.getArchiveHash())){
          LOGGER.log(S3_MD5_HASH_FAILED, localFileName, s3Record.getArchiveHash(), base);
          throw new IOException(s3Record.getDigestName()+" mismatch Computed:"+base+" original "+s3Record.getArchiveHash());
        }
      }
      amazonS3.deleteObject(s3Record.getBucketName(), s3Record.getEntryName());
      LOGGER.log(S3_RESTORED_DATA, localFileName, bucketName);
    } catch (AmazonS3Exception e) {
      throw new IOException(e);
    }
  }

  public S3Record archive(String path, String localFileName, MessageDigest messageDigest) throws IOException {
    File file = new File(localFileName);
    String digest = null;
    if(compress) {
      File zip = new File(localFileName + "_zip");
      FileCompressionProcessor fileCompressionProcessor = new FileCompressionProcessor();
      fileCompressionProcessor.in(file, zip, messageDigest);
      file = zip;
      if(messageDigest != null) {
        digest = Base64.getEncoder().encodeToString(messageDigest.digest());
      }
    }
    else{
      digest = computeFileHash(file, messageDigest);
    }

    String entryName = path+"/"+file.getName();
    amazonS3.putObject(bucketName, entryName, file);
    S3Record s3Record = new S3Record(bucketName, entryName, digest, file.length(), compress);
    LOGGER.log(S3_ARCHIVING_DATA, localFileName, bucketName);
    FileHelper.delete(file);
    return s3Record;
  }


  private String computeFileHash(File file, MessageDigest messageDigest) throws IOException {
    if(messageDigest != null) {
      byte[] tmp = new byte[10240];
      try (FileInputStream fileInputStream = new FileInputStream(file)) {
        int read =1;
        while(read > 0) {
          read = fileInputStream.read(tmp, 0, tmp.length);
          if (read > 0) {
            messageDigest.update(tmp, 0,  Math.max(read, messageDigest.getDigestLength()));
          }
        }
        return Base64.getEncoder().encodeToString(messageDigest.digest());
      }
    }
    return "";
  }
}
