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
import io.mapsmessaging.storage.impl.file.partition.IndexRecord;
import java.io.Closeable;
import java.io.IOException;

public interface DataStorage<T extends Storable> extends Closeable {

  @Override
  void close() throws IOException;

  String getName();

  void delete() throws IOException;

  IndexRecord add(T object) throws IOException;

  T get(IndexRecord item) throws IOException;

  long length() throws IOException;

  boolean isValidationRequired();

  boolean isFull();
}