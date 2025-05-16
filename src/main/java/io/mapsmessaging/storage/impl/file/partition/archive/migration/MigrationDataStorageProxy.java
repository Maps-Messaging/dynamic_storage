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

package io.mapsmessaging.storage.impl.file.partition.archive.migration;

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.impl.file.FileHelper;
import io.mapsmessaging.storage.impl.file.PartitionStorageConfig;
import io.mapsmessaging.storage.impl.file.partition.DataStorageImpl;
import io.mapsmessaging.storage.impl.file.partition.archive.ArchiveRecord;
import io.mapsmessaging.storage.impl.file.partition.archive.DataStorageProxy;
import io.mapsmessaging.storage.impl.file.partition.archive.DataStorageStub;
import io.mapsmessaging.storage.impl.file.partition.archive.compress.FileCompressionProcessor;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

public class MigrationDataStorageProxy<T extends Storable> extends DataStorageProxy<T> {

  private final String destination;

  public MigrationDataStorageProxy(PartitionStorageConfig<T> config) throws IOException {
    super(config);
    String t = config.getMigrationDestination();
    if(t.endsWith(File.separator)){
      t = t.substring(0, t.length()-1);
    }
    destination = t;
  }

  @Override
  protected ArchiveRecord buildArchiveRecord() {
    return new MigrationRecord();
  }

  @Override
  public String getArchiveName() {
    return "Migrate";
  }

  @Override
  public void delete() throws IOException {
    if (isArchived) {
      super.delete();
      FileHelper.delete(destination+File.separator+fileName+"_zip", true, false);
    } else {
      physicalStore.delete();
    }
  }

  public void archive() throws IOException {
    if (!isArchived) {
      File from = new File(fileName);
      File to = new File(destination+File.separator+fileName+"_zip");
      try {
        MessageDigest messageDigest = getMessageDigest();
        FileCompressionProcessor compressionHelper = new FileCompressionProcessor();
        Files.createDirectories(to.getParentFile().toPath());
        long length = compressionHelper.in(from, to, messageDigest);
        String hash = null;
        if(messageDigest != null){
          hash =  Base64.getEncoder().encodeToString(messageDigest.digest());
        }
        MigrationRecord migrationRecord = new MigrationRecord(length, hash, digestName);
        migrationRecord.write(fileName);
        physicalStore = new DataStorageStub<>(migrationRecord);
        isArchived = true;
      } catch (NoSuchAlgorithmException e) {
        throw new IOException(e);
      }
    }
  }

  public void restore() throws IOException {
    try {
      File to = new File(fileName);
      File from = new File(destination+File.separator+fileName+"_zip");
      FileCompressionProcessor compressionHelper = new FileCompressionProcessor();
      FileHelper.delete(to);
      MigrationRecord migrationRecord = (MigrationRecord) ((DataStorageStub<T>)physicalStore).getArchiveRecord();
      MessageDigest messageDigest = getMessageDigest(migrationRecord.getDigestName());
      compressionHelper.out(from, to, messageDigest);
      String digest = null;
      if(messageDigest != null) {
        digest = Base64.getEncoder().encodeToString(messageDigest.digest());
        if(!digest.equals(migrationRecord.getArchiveHash())){
          throw new IOException("File has been changed, MD5 hash does not match");
        }
      }
      physicalStore = new DataStorageImpl<>(fileName, storableFactory, sync, maxPartitionSize);
      isArchived = false;
    } catch (NoSuchAlgorithmException e) {
      throw new IOException(e);
    }
  }

}