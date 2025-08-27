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

import io.mapsmessaging.logging.Logger;
import io.mapsmessaging.logging.LoggerFactory;
import io.mapsmessaging.storage.impl.file.FileHelper;
import io.mapsmessaging.storage.impl.file.partition.deferred.StreamProcessor;
import io.mapsmessaging.storage.impl.file.partition.deferred.compress.FileCompressionProcessor;
import io.mapsmessaging.storage.impl.file.partition.deferred.compress.StreamCompressionHelper;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.Base64;

import static io.mapsmessaging.storage.logging.StorageLogMessages.*;

public class S3TransferApi {
  private static final Logger LOGGER = LoggerFactory.getLogger(S3TransferApi.class);

  private final S3Client s3Client;
  private final String bucketName;
  private final boolean compress;

  public S3TransferApi(S3Client s3Client, String bucketName, boolean compress){
    this.s3Client = s3Client;
    this.bucketName = bucketName;
    this.compress = compress;
  }
  public void delete(S3Record s3Record){
    s3Client.deleteObject(DeleteObjectRequest.builder()
        .bucket(s3Record.getBucketName())
        .key(s3Record.getEntryName())
        .build());
    LOGGER.log(S3_ENTITY_DELETED, s3Record.getEntryName());
  }

  public void retrieve(String localFileName, S3Record s3Record, MessageDigest messageDigest) throws IOException {
    File file = new File(localFileName);
    if (!FileHelper.delete(localFileName)) {
      LOGGER.log(S3_FILE_DELETE_FAILED, localFileName);
      throw new IOException("Unable to delete placeholder file");
    }

    try {
      GetObjectRequest request = GetObjectRequest.builder()
          .bucket(s3Record.getBucketName())
          .key(s3Record.getEntryName())
          .build();

      try (ResponseInputStream<GetObjectResponse> s3Stream = s3Client.getObject(request);
           FileOutputStream fileOutputStream = new FileOutputStream(file, false)) {

        StreamProcessor streamProcessor = s3Record.isCompressed() ? new StreamCompressionHelper() : new StreamProcessor();
        streamProcessor.out(s3Stream, fileOutputStream, messageDigest);
      }

      if (messageDigest != null) {
        byte[] hash = messageDigest.digest();
        String base = Base64.getEncoder().encodeToString(hash);
        if (!base.equals(s3Record.getDeferredHash())) {
          LOGGER.log(S3_MD5_HASH_FAILED, localFileName, s3Record.getDeferredHash(), base);
          throw new IOException(s3Record.getDigestName() + " mismatch Computed:" + base + " original " + s3Record.getDeferredHash());
        }
      }

      delete(s3Record);
      LOGGER.log(S3_RESTORED_DATA, localFileName, bucketName);
    } catch (S3Exception e) {
      throw new IOException(e);
    }
  }

  public S3Record archive(String path, String localFileName, MessageDigest messageDigest) throws IOException {
    File file = new File(localFileName);
    String digest = null;

    if (compress) {
      File zip = new File(localFileName + "_zip");
      FileCompressionProcessor fileCompressionProcessor = new FileCompressionProcessor();
      fileCompressionProcessor.in(file, zip, messageDigest);
      file = zip;
      if (messageDigest != null) {
        digest = Base64.getEncoder().encodeToString(messageDigest.digest());
      }
    } else {
      digest = computeFileHash(file, messageDigest);
    }

    String entryName = path + "/" + file.getName();

    PutObjectRequest request = PutObjectRequest.builder()
        .bucket(bucketName)
        .key(entryName)
        .build();

    s3Client.putObject(request, RequestBody.fromFile(file));

    S3Record s3Record = new S3Record(bucketName, entryName, digest, file.length(), compress);
    LOGGER.log(S3_ARCHIVING_DATA, localFileName, bucketName);
    FileHelper.delete(file);
    return s3Record;
  }

  private String computeFileHash(File file, MessageDigest messageDigest) throws IOException {
    if (messageDigest != null) {
      byte[] tmp = new byte[10240];
      try (FileInputStream fileInputStream = new FileInputStream(file)) {
        int read;
        while ((read = fileInputStream.read(tmp)) > 0) {
          messageDigest.update(tmp, 0, read);
        }
        return Base64.getEncoder().encodeToString(messageDigest.digest());
      }
    }
    return "";
  }
}