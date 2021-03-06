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

package io.mapsmessaging.storage.impl;

import io.mapsmessaging.storage.ExpiredStorableHandler;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.StorableFactory;
import io.mapsmessaging.storage.StorageFactory;
import java.util.Map;

public abstract class BaseStorageFactory<T extends Storable> implements StorageFactory<T> {

  protected Map<String, String> properties;
  protected StorableFactory<T> storableFactory;
  protected ExpiredStorableHandler expiredHandler;

  protected BaseStorageFactory() {
  }

  protected BaseStorageFactory(Map<String, String> properties, StorableFactory<T> storableFactory, ExpiredStorableHandler expiredHandler) {
    this.properties = properties;
    this.storableFactory = storableFactory;
    this.expiredHandler = expiredHandler;
  }

}
