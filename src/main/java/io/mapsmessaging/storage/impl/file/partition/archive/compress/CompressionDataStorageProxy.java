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
import io.mapsmessaging.storage.StorableFactory;
import io.mapsmessaging.storage.impl.file.partition.ArchivedDataStorage;
import io.mapsmessaging.storage.impl.file.partition.DataStorage;
import io.mapsmessaging.storage.impl.file.partition.DataStorageImpl;
import io.mapsmessaging.storage.impl.file.partition.IndexRecord;
import io.mapsmessaging.storage.impl.file.partition.archive.DataStorageStub;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class CompressionDataStorageProxy<T extends Storable> implements ArchivedDataStorage<T> {

  private final String fileName;
  private final StorableFactory<T> storableFactory;
  private final boolean sync;
  private final long maxPartitionSize;

  private DataStorage<T> physicalStore;
  private boolean isArchived;

  public CompressionDataStorageProxy(String fileName, StorableFactory<T> storableFactory, boolean sync, long maxPartitionSize) throws IOException {
    this.fileName = fileName;
    this.sync = sync;
    this.storableFactory = storableFactory;
    this.maxPartitionSize = maxPartitionSize;

    File file = new File(fileName);
    isArchived = false;
    if (!file.exists()) {
      physicalStore = new DataStorageImpl<>(fileName, storableFactory, sync, maxPartitionSize);
    } else {
      physicalStore = detectAndLoad();
    }
  }

  private DataStorage<T> detectAndLoad() throws IOException {
    try (FileInputStream fileInputStream = new FileInputStream(fileName)) {
      byte[] tmp = fileInputStream.readNBytes(16);
      int test = tmp[0] & 0xff;
      isArchived = (test != 0xEF && test != 0x00);
    }
    if (!isArchived) {
      return new DataStorageImpl<>(fileName, storableFactory, sync, maxPartitionSize);
    }
    CompressionRecord zipRecord = new CompressionRecord();
    zipRecord.read(fileName);
    return new DataStorageStub<>(zipRecord);
  }

  @Override
  public void close() throws IOException {
    physicalStore.close();
  }

  public void pause() throws IOException {
    if (!isArchived) {
      physicalStore.close();
    }
  }

  public void resume() throws IOException {
    if (!isArchived) {
      physicalStore = new DataStorageImpl<>(fileName, storableFactory, sync, maxPartitionSize);
    }
  }

  @Override
  public String getArchiveName() {
    return "S3";
  }

  @Override
  public String getName() {
    return fileName;
  }

  @Override
  public void delete() throws IOException {
    if (isArchived) {
      File file = new File(fileName);
      File zipFile = new File(fileName+"_zip");
      file.delete();
      zipFile.delete();
    } else {
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
    if (!isArchived) {
      CompressionHelper compressionHelper = new CompressionHelper();
      long length = compressionHelper.compress(fileName);
      CompressionRecord compressionRecord = new CompressionRecord(length);
      compressionRecord.write(fileName);
      physicalStore = new DataStorageStub<>(compressionRecord);
      isArchived = true;
    }
  }

  public void restore() throws IOException {
    CompressionHelper helper = new CompressionHelper();
    File placeHolder = new File(fileName);
    placeHolder.delete();
    helper.decompress(fileName);
    physicalStore = new DataStorageImpl<>(fileName, storableFactory, sync, maxPartitionSize);
    isArchived = false;
  }

  @Override
  public boolean isArchived() {
    return isArchived;
  }

  @Override
  public boolean supportsArchiving() {
    return true;
  }

  private void loadIfArchived() throws IOException {
    if (isArchived) {
      restore();
    }
  }
}