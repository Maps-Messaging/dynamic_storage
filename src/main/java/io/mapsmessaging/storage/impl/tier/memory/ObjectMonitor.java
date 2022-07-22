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

package io.mapsmessaging.storage.impl.tier.memory;

import io.mapsmessaging.storage.Storable;
import lombok.Getter;
import lombok.Setter;

class ObjectMonitor<T extends Storable> implements Storable {

  private final T storable;

  private @Getter
  @Setter
  long lastAccess;

  ObjectMonitor() {
    storable = null;
  }

  public ObjectMonitor(T storable) {
    this.storable = storable;
    lastAccess = System.currentTimeMillis();
  }

  public T getStorable() {
    return storable;
  }

  @Override
  public long getKey() {
    return storable.getKey();
  }

  @Override
  public long getExpiry() {
    return storable.getExpiry();
  }
}