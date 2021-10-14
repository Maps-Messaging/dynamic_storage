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

package io.mapsmessaging.storage;

import io.mapsmessaging.storage.impl.file.TaskQueue;
import io.mapsmessaging.utilities.threads.tasks.TaskScheduler;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public interface Storage<T extends Storable> extends Closeable {

  //region Life cycle API
  void delete() throws IOException;

  default void shutdown()throws IOException{}
  //endregion

  //region Administration API
  String getName();

  long size() throws IOException;

  long getLastKey();

  long getLastAccess();
  default void updateLastAccess(){};

  boolean isEmpty();

  TaskQueue getTaskScheduler();

  //<editor-fold desc="API for pause/resume operations. If supported the pause will close all file descriptors but maintain the inmemory state of the store">
  default boolean supportPause(){
    return false;
  }
  default void pause()throws IOException{}
  default void resume()throws IOException{}
  //</editor-fold>

  default boolean isCacheable(){
    return true;
  }

  // Returns a list of events NOT found but was in the to keep list
  @NotNull List<Long> keepOnly(@NotNull List<Long> listToKeep) throws IOException;

  @NotNull Statistics getStatistics();
  //endregion

  //region Storage access API
  void add(@NotNull T object) throws IOException;

  boolean remove(long key) throws IOException;

  @Nullable T get(long key) throws IOException;

  //endregion


  //region Scheduler control API
  @SuppressWarnings("java:S112")
  default boolean executeTasks() throws Exception {
    return false;
  }


  void setExecutor(TaskScheduler executor);
  //endregion
}
