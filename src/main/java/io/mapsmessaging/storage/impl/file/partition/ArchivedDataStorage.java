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

package io.mapsmessaging.storage.impl.file.partition;

import io.mapsmessaging.storage.Storable;
import java.io.IOException;

public interface ArchivedDataStorage<T extends Storable> extends DataStorage<T>{

  String getArchiveName();

  void pause() throws IOException;

  void resume() throws IOException;

  void archive() throws IOException;

  void restore() throws IOException;

  boolean isArchived();

  boolean supportsArchiving();
}
