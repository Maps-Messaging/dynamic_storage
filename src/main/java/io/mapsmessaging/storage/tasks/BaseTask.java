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

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.Callable;

public abstract class BaseTask<T extends Storable, V> implements Callable<V> {

  protected final @Getter
  Storage<T> storage;
  protected final @Getter
  Completion<V> completion;

  protected BaseTask(@NotNull Storage<T> storage, Completion<V> completion) {
    this.storage = storage;
    this.completion = completion;
  }

  @SuppressWarnings("java:S112") // we are an abstracted function, we can expect any exception to be raised here
  protected abstract V execute() throws Exception;

  @Override
  public V call() throws Exception {
    V result = null;
    Exception exception = null;
    try {
      result = execute();
    } catch (Exception e) {
      e.printStackTrace();
      exception = e;
    }
    if (completion != null) {
      if (result != null) {
        completion.onCompletion(result);
      }
      if (exception != null) {
        completion.onException(exception);
        throw exception;
      }
    }
    return result;
  }

}
