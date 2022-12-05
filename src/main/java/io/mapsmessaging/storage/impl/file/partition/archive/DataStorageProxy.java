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

package io.mapsmessaging.storage.impl.file.partition.archive;

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.StorableFactory;
import io.mapsmessaging.storage.impl.file.PartitionStorageConfig;
import io.mapsmessaging.storage.impl.file.partition.ArchivedDataStorage;
import io.mapsmessaging.storage.impl.file.partition.DataStorage;
import io.mapsmessaging.storage.impl.file.partition.DataStorageImpl;
import io.mapsmessaging.storage.impl.file.partition.IndexRecord;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.jetbrains.annotations.Nullable;

public abstract class DataStorageProxy<T extends Storable> implements ArchivedDataStorage<T> {

  protected final String digestName;
  protected final String fileName;
  protected final StorableFactory<T> storableFactory;
  protected final boolean sync;
  protected final long maxPartitionSize;

  protected DataStorage<T> physicalStore;
  protected boolean isArchived;

  protected DataStorageProxy(PartitionStorageConfig<T> config) throws IOException {
    fileName = config.getFileName()+ "_data";
    sync = config.isSync();
    storableFactory = config.getStorableFactory();
    maxPartitionSize = config.getMaxPartitionSize();
    digestName = config.getDigestName();
    File file = new File(fileName);
    isArchived = false;
    if (!file.exists()) {
      physicalStore = new DataStorageImpl<>(fileName, storableFactory, sync, maxPartitionSize);
    } else {
      physicalStore = detectAndLoad();
    }
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
  public String getName() {
    return fileName;
  }

  @Override
  public void delete() throws IOException {
    if (isArchived) {
      File file = new File(fileName);
      file.delete();
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

  protected @Nullable MessageDigest getMessageDigest() throws NoSuchAlgorithmException {
    return getMessageDigest(digestName);
  }

  protected @Nullable MessageDigest getMessageDigest(String name) throws NoSuchAlgorithmException {
    if (name != null && name.length() > 0 && !name.equalsIgnoreCase("none")) {
      return MessageDigest.getInstance(name);
    }
    return null;
  }

  protected abstract ArchiveRecord buildArchiveRecord();

  private DataStorage<T> detectAndLoad() throws IOException {
    try (FileInputStream fileInputStream = new FileInputStream(fileName)) {
      byte[] tmp = fileInputStream.readNBytes(16);
      int test = tmp[0] & 0xff;
      isArchived = (test != 0xEF && test != 0x00);
    }
    if (!isArchived) {
      return new DataStorageImpl<>(fileName, storableFactory, sync, maxPartitionSize);
    }
    ArchiveRecord archiveRecord = buildArchiveRecord();
    archiveRecord.read(fileName);
    return new DataStorageStub<>(archiveRecord);
  }
}