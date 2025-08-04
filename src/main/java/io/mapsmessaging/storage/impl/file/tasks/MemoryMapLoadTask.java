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

package io.mapsmessaging.storage.impl.file.tasks;

import io.mapsmessaging.storage.impl.file.partition.IndexManager;

@SuppressWarnings("javaarchitecture:S7091") // yes uses te IndexManager
public class MemoryMapLoadTask  implements FileTask<Boolean> {

  private final IndexManager indexStorage;
  private final boolean walkIndex;

  public MemoryMapLoadTask(IndexManager indexStorage, boolean walkIndex) {
    this.indexStorage = indexStorage;
    this.walkIndex = walkIndex;
  }

  @Override
  public boolean canCancel() {
    return false;
  }

  @Override
  public Boolean call() {
    indexStorage.loadMap(walkIndex);
    return true;
  }
}
