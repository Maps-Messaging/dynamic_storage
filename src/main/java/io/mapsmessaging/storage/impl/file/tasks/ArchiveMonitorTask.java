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
import java.util.concurrent.atomic.AtomicBoolean;

import static io.mapsmessaging.storage.logging.StorageLogMessages.ARCHIVE_MONITOR_FAILED;

@SuppressWarnings("javaarchitecture:S7091") // yes it uses the PartitionStorage class to work on
public class ArchiveMonitorTask<T extends Storable> implements FileTask<Boolean>, Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ArchiveMonitorTask.class);

  private final PartitionStorage<T> storage;
  private final AtomicBoolean pending = new AtomicBoolean();

  public ArchiveMonitorTask(PartitionStorage<T> storage) {
    this.storage = storage;
  }

  @Override
  public Boolean call() throws IOException {
    try {
      storage.scanForArchiveMigration();
      return true;
    } catch (IOException e) {
      LOGGER.log(ARCHIVE_MONITOR_FAILED, e, storage.getName());
      throw e;
    } finally {
      pending.set(false);
    }
  }

  public void run(){
    if (!pending.compareAndSet(false, true)) {
      return;
    }
    try {
      storage.getTaskScheduler().submit(this);
    } catch (IOException e) {
      pending.set(false);
      LOGGER.log(ARCHIVE_MONITOR_FAILED, e, storage.getName());
    }
  }

  @Override
  public boolean canCancel() {
    return false;
  }

}
