/*
 *
 * Copyright [2020 - 2021]   [Matthew Buckton]
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 *
 */

package io.mapsmessaging.storage.impl;

import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.storage.impl.cache.Cache;
import io.mapsmessaging.storage.impl.cache.CacheLayer;
import io.mapsmessaging.storage.impl.cache.weak.WeakReferenceCacheStorage;

public class WeakReferenceTest extends BaseLayeredTest{

  @Override
  public Storage<MappedData> createStore(Storage<MappedData> storage) {
    Cache<MappedData> cache = new WeakReferenceCacheStorage<>(storage.getName());
    return new CacheLayer<>(false, cache, storage);
  }
}
