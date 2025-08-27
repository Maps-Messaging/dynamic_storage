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

package io.mapsmessaging.storage;

import io.mapsmessaging.logging.Logger;
import io.mapsmessaging.logging.LoggerFactory;
import io.mapsmessaging.storage.logging.StorageLogMessages;

import java.io.IOException;
import java.util.Queue;

public class BaseExpiredHandler<T extends Storable> implements ExpiredStorableHandler {

  private final Logger logger = LoggerFactory.getLogger(BaseExpiredHandler.class);
  private final Storage<T> storage;

  public BaseExpiredHandler(Storage<T> storage) {
    this.storage = storage;
  }

  @Override
  public void expired(Queue<Long> listOfExpiredEntries) throws IOException {
    for (Long remove : listOfExpiredEntries) {
      if(logger.isTraceEnabled()) logger.log(StorageLogMessages.REMOVING_EXPIRED_ENTRY, remove, storage.getName());
      storage.remove(remove);
    }
  }
}
