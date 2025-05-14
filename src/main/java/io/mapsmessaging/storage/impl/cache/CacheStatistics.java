/*
 *    Copyright [ 2020 - 2024 ] [Matthew Buckton]
 *    Copyright [ 2024 - 2025 ] [Maps Messaging B.V.]
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
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

package io.mapsmessaging.storage.impl.cache;

import io.mapsmessaging.storage.Statistics;
import lombok.Getter;

public class CacheStatistics implements Statistics {

  @Getter
  private final long miss;
  @Getter
  private final long hit;
  @Getter
  private final int size;
  @Getter
  private final Statistics storageStatistics;

  public CacheStatistics(long miss, long hit, int size, Statistics storageStatistics) {
    this.miss = miss;
    this.hit = hit;
    this.size = size;
    this.storageStatistics = storageStatistics;
  }

  @Override
  public String toString() {
    return getStorageStatistics().toString() + ",\tCache Hits:" + getHit() + ",\t Cache Miss:" + getHit() + ",\t Cache Size:" + getSize();
  }
}
