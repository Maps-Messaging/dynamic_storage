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

package io.mapsmessaging.storage.impl.file.partition.deferred;

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.impl.file.partition.DataStorage;
import io.mapsmessaging.storage.impl.file.partition.IndexRecord;
import lombok.Getter;

import java.io.IOException;

public class DataStorageStub<T extends Storable> implements DataStorage<T> {

  private static final String ERROR_MESSAGE = "This should not be called, the file needs to be restored";

  @Getter
  private final DeferredRecord deferredRecord;

  public DataStorageStub(DeferredRecord deferredRecord) {
    this.deferredRecord = deferredRecord;
  }

  @Override
  public void close() throws IOException {
    // We have nothing to close!!
  }

  @Override
  public String getName() {
    return null;
  }

  @Override
  public void delete() throws IOException {
    throw new IOException(ERROR_MESSAGE);
  }

  @Override
  public IndexRecord add(T object) throws IOException {
    throw new IOException(ERROR_MESSAGE);
  }

  @Override
  public T get(IndexRecord item) throws IOException {
    throw new IOException(ERROR_MESSAGE);
  }

  @Override
  public long length() throws IOException {
    return deferredRecord.getLength();
  }

  @Override
  public boolean isValidationRequired() {
    return false;
  }

  @Override
  public boolean isFull() {
    return true;
  }
}