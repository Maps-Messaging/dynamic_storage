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

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.impl.file.config.PartitionStorageConfig;
import io.mapsmessaging.storage.impl.file.partition.DataStorageImpl;
import io.mapsmessaging.storage.impl.file.partition.deferred.DataStorageProxy;
import io.mapsmessaging.storage.impl.file.partition.deferred.DataStorageStub;
import io.mapsmessaging.storage.impl.file.partition.deferred.DeferredRecord;

import java.io.File;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class S3DataStorageProxy<T extends Storable> extends DataStorageProxy<T> {

  private final S3TransferApi s3TransferApi;

  public S3DataStorageProxy(S3TransferApi transferApi, PartitionStorageConfig config) throws IOException {
    super(config);
    this.s3TransferApi = transferApi;
  }

  @Override
  public void delete() throws IOException {
    if(isArchived) {
      super.delete();
      S3Record s3Record =(S3Record) ((DataStorageStub<T>)physicalStore).getDeferredRecord();
      s3TransferApi.delete(s3Record); // Delete the S3 entry
    }
    else {
      physicalStore.delete();
    }
  }

  @Override
  protected DeferredRecord buildArchiveRecord() {
    return new S3Record();
  }

  public void archive() throws IOException {
    if(!isArchived){
      File file = new File(fileName);
      try {
        MessageDigest messageDigest = getMessageDigest();
        S3Record s3Record = s3TransferApi.archive(file.getParentFile().getPath(), fileName, messageDigest);
        s3Record.setDigestName(digestName);
        s3Record.write(fileName);
        physicalStore = new DataStorageStub<>(s3Record);
        isArchived = true;
      } catch (NoSuchAlgorithmException e) {
        throw new IOException(e);
      }
    }
  }

  public void restore() throws IOException {
    S3Record s3Record = (S3Record) ((DataStorageStub<T>)physicalStore).getDeferredRecord();
    try {
      MessageDigest messageDigest = getMessageDigest(s3Record.getDigestName());
      s3TransferApi.retrieve(fileName, s3Record, messageDigest);
      physicalStore = new DataStorageImpl<>(fileName, storableFactory, sync, maxPartitionSize);
      isArchived = false;
    } catch (NoSuchAlgorithmException e) {
      throw new IOException(e);
    }
  }
}
