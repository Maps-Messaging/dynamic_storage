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

package io.mapsmessaging.storage.impl.file.partition.base;

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.StorableFactory;
import io.mapsmessaging.storage.impl.file.config.PartitionStorageConfig;
import io.mapsmessaging.storage.impl.file.partition.DataStorage;
import io.mapsmessaging.storage.impl.file.partition.DataStorageImpl;
import io.mapsmessaging.storage.impl.file.partition.DeferredDataStorage;
import io.mapsmessaging.storage.impl.file.partition.IndexRecord;

import java.io.IOException;

public class BaseDataStorage <T extends Storable> implements DeferredDataStorage<T> {

  private final String fileName;
  private final StorableFactory<T> storableFactory;
  private final boolean sync;
  private final long maxPartitionSize;

  private DataStorage<T> physicalStore;

  public BaseDataStorage(PartitionStorageConfig config) throws IOException {
    this.fileName = config.getFileName()+ "_data";
    this.storableFactory = config.getStorableFactory();
    this.sync = config.isSync();
    this.maxPartitionSize = config.getMaxPartitionSize();
    physicalStore = new DataStorageImpl<>(fileName, storableFactory, sync, maxPartitionSize);
  }

  @Override
  public void close() throws IOException {
    physicalStore.close();
  }

  public void pause() throws IOException {
    close();
  }

  public void resume() throws IOException {
    physicalStore = new DataStorageImpl<>(fileName, storableFactory, sync, maxPartitionSize);
  }

  @Override
  public String getName() {
    return fileName;
  }

  @Override
  public void delete() throws IOException {
    physicalStore.delete();
  }

  @Override
  public IndexRecord add(T object) throws IOException {
    return physicalStore.add(object);
  }

  @Override
  public T get(IndexRecord item) throws IOException {
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

  public void archive() {
    // No Archive options are supported for the base functionality
  }

  public void restore() {
    // No Archive options are supported for the base functionality
  }

  @Override
  public boolean isArchived() {
    return false;
  }

  @Override
  public boolean supportsArchiving() {
    return false;
  }

}
