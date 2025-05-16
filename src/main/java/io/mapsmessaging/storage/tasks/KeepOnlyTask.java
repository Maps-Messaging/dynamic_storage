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

package io.mapsmessaging.storage.tasks;

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;

public class KeepOnlyTask<T extends Storable> extends BaseTask<T, Collection<Long>> {

  private final Collection<Long> toKeep;

  public KeepOnlyTask(@NotNull Storage<T> storage, Collection<Long> toKeep, Completion<Collection<Long>> completion) {
    super(storage, completion);
    this.toKeep = toKeep;
  }

  @Override
  public Collection<Long> execute() throws Exception {
    return storage.keepOnly(toKeep);
  }
}