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

import io.mapsmessaging.logging.Logger;
import io.mapsmessaging.logging.LoggerFactory;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.impl.file.PartitionStorage;

import java.io.IOException;

import static io.mapsmessaging.storage.logging.StorageLogMessages.ARCHIVE_MONITOR_FAILED;

public class ArchiveMonitorTask<T extends Storable> implements FileTask<Boolean>, Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ArchiveMonitorTask.class);

  private final PartitionStorage<T> storage;

  public ArchiveMonitorTask(PartitionStorage<T> storage) {
    this.storage = storage;
  }

  @Override
  public Boolean call() throws IOException {
    storage.scanForArchiveMigration();
    return true;
  }

  public void run(){
    try {
      call();
    } catch (IOException e) {
      LOGGER.log(ARCHIVE_MONITOR_FAILED, e, storage.getName());
    }
  }

  @Override
  public boolean canCancel() {
    return false;
  }

}
