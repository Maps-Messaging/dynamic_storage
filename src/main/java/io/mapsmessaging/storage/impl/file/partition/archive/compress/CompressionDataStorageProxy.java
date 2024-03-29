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

package io.mapsmessaging.storage.impl.file.partition.archive.compress;

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.impl.file.FileHelper;
import io.mapsmessaging.storage.impl.file.PartitionStorageConfig;
import io.mapsmessaging.storage.impl.file.partition.DataStorageImpl;
import io.mapsmessaging.storage.impl.file.partition.archive.ArchiveRecord;
import io.mapsmessaging.storage.impl.file.partition.archive.DataStorageProxy;
import io.mapsmessaging.storage.impl.file.partition.archive.DataStorageStub;
import java.io.File;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

public class CompressionDataStorageProxy<T extends Storable> extends DataStorageProxy<T> {

  public CompressionDataStorageProxy(PartitionStorageConfig<T> config) throws IOException {
    super(config);
  }

  @Override
  protected ArchiveRecord buildArchiveRecord() {
    return new CompressionRecord();
  }

  @Override
  public String getArchiveName() {
    return "Compress";
  }

  @Override
  public void delete() throws IOException {
    if (isArchived) {
      super.delete();
      FileHelper.delete(fileName+"_zip");
    }
  }

  public void archive() throws IOException {
    if (!isArchived) {
      FileCompressionProcessor compressionHelper = new FileCompressionProcessor();
      File source = new File(fileName);
      File zipped = new File(fileName+"_zip");
      CompressionRecord compressionRecord = null;
      try {
        MessageDigest messageDigest = getMessageDigest();
        long length = compressionHelper.in(source, zipped, messageDigest);
        String hash = "";
        if(messageDigest != null){
          hash = Base64.getEncoder().encodeToString(messageDigest.digest());
        }
        compressionRecord = new CompressionRecord(length, hash, digestName);
        compressionRecord.write(fileName);
      } catch (NoSuchAlgorithmException e) {
        throw new IOException(e);
      }
      physicalStore = new DataStorageStub<>(compressionRecord);
      isArchived = true;
    }
  }

  public void restore() throws IOException {
    FileCompressionProcessor compressionHelper = new FileCompressionProcessor();
    FileHelper.delete(fileName);
    File zipped = new File(fileName+"_zip");
    File destination = new File(fileName);
    try {
      CompressionRecord compressionRecord = (CompressionRecord) ((DataStorageStub<T>)physicalStore).getArchiveRecord();
      MessageDigest messageDigest = getMessageDigest(compressionRecord.getDigestName());
      compressionHelper.out(zipped, destination, messageDigest);
      if(messageDigest != null) {
        String computed = Base64.getEncoder().encodeToString(messageDigest.digest());
        if (!computed.equals(compressionRecord.getArchiveHash())) {
          throw new IOException("MD5 hash does not match");
        }
      }
      physicalStore = new DataStorageImpl<>(fileName, storableFactory, sync, maxPartitionSize);
      isArchived = false;
    }
    catch (NoSuchAlgorithmException e) {
      throw new IOException(e);
    }
  }
}