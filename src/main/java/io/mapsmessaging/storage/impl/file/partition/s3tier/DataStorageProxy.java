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

package io.mapsmessaging.storage.impl.file.partition.s3tier;

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.StorableFactory;
import io.mapsmessaging.storage.impl.file.partition.DataStorage;
import io.mapsmessaging.storage.impl.file.partition.DataStorageImpl;
import io.mapsmessaging.storage.impl.file.partition.IndexRecord;
import io.mapsmessaging.storage.impl.file.s3.S3Record;
import io.mapsmessaging.storage.impl.file.s3.S3TransferApi;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class DataStorageProxy<T extends Storable> implements DataStorage<T> {

  private final String fileName;
  private final StorableFactory<T> storableFactory;
  private final boolean sync;
  private final long maxPartitionSize;

  private final S3TransferApi s3TransferApi;

  private DataStorage<T> physicalStore;
  private boolean isArchived;

  public DataStorageProxy(S3TransferApi transferApi, String fileName, StorableFactory<T> storableFactory, boolean sync, long maxPartitionSize) throws IOException {
    this.fileName = fileName;
    this.sync = sync;
    this.storableFactory = storableFactory;
    this.maxPartitionSize = maxPartitionSize;
    this.s3TransferApi = transferApi;

    File file = new File(fileName);
    isArchived = false;
    if(!file.exists()){
      physicalStore = new DataStorageImpl<>(fileName, storableFactory, sync, maxPartitionSize);
    }
    else{
      physicalStore = detectAndLoad();
    }
  }

  private DataStorage<T> detectAndLoad() throws IOException {
    try (FileInputStream fileInputStream = new FileInputStream(fileName)) {
      byte[] tmp = fileInputStream.readNBytes(16);
      int test = tmp[0] & 0xff;
      isArchived = (test != 0x0 && test != 0xff);
    }
    if(!isArchived){
      return new DataStorageImpl<>(fileName, storableFactory, sync, maxPartitionSize);
    }
    S3Record s3Record = new S3Record();
    s3Record.read(fileName);
    return new S3DataStorageStub<>(s3Record);
  }

  @Override
  public void close() throws IOException {
    physicalStore.close();
  }

  public void pause() throws IOException {
    if(!isArchived) {
      physicalStore.close();
    }
  }

  public void resume() throws IOException {
    if(!isArchived) {
      physicalStore = new DataStorageImpl<>(fileName, storableFactory, sync, maxPartitionSize);
    }
  }

  @Override
  public String getName() {
    return fileName;
  }

  @Override
  public void delete() throws IOException {
    if(isArchived) {
      File file = new File(fileName);
      if (!file.delete()) { // Delete local reference
        System.err.println("Failed to delete");
      }
      S3Record s3Record = ((S3DataStorageStub<T>)physicalStore).getS3Record();
      s3TransferApi.delete(s3Record); // Delete the S3 entry
    }
    else {
      physicalStore.delete();
    }
  }

  @Override
  public IndexRecord add(T object) throws IOException {
    loadIfArchived();
    return physicalStore.add(object);
  }

  @Override
  public T get(IndexRecord item) throws IOException {
    loadIfArchived();
    return physicalStore.get(item);
  }

  @Override
  public long length() throws IOException {
    return physicalStore.length();
  }

  @Override
  public boolean isValidationRequired() {
    return physicalStore.isValidationRequired();
  }

  @Override
  public boolean isFull() {
    return physicalStore.isFull();
  }

  public void archive() throws IOException {
    if(!isArchived){
      File file = new File(fileName);
      S3Record s3Record = s3TransferApi.archive(file.getParentFile().getPath(), fileName);
      physicalStore = new S3DataStorageStub<>(s3Record);
      isArchived = true;
    }
  }

  private void loadIfArchived() throws IOException {
    if(isArchived){
      S3Record s3Record = ((S3DataStorageStub<T>)physicalStore).getS3Record();
      s3TransferApi.retrieve(fileName, s3Record);
    }
  }
}
