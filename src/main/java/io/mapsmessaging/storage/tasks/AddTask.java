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

package io.mapsmessaging.storage.tasks;

import io.mapsmessaging.storage.LayeredStorage;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import org.jetbrains.annotations.NotNull;

public class AddTask<T extends Storable> extends BaseTask<T, T> {

  private final T toStore;

  public AddTask(@NotNull Storage<T> storage, T toStore, Completion<T> completion) {
    super(storage, completion);
    this.toStore = toStore;
  }

  @Override
  public T execute() throws Exception {
    if(storage instanceof LayeredStorage){
      ((LayeredStorage<T>)storage).add(toStore, completion);
    }
    else {
      storage.add(toStore);
    }
    return toStore;
  }
}
