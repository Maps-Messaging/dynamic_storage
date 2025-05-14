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

package io.mapsmessaging.storage.logging;

import io.mapsmessaging.logging.Category;
import io.mapsmessaging.logging.LEVEL;
import io.mapsmessaging.logging.LogMessage;
import lombok.Getter;

public enum StorageLogMessages implements LogMessage {

  //region Async-Storage-Api
  ASYNC_STORAGE_CREATED(LEVEL.TRACE, STORAGE_CATEGORY.ASYNC, "Async layer constructed over {}"),
  ASYNC_CLOSE_REQUESTED(LEVEL.TRACE, STORAGE_CATEGORY.ASYNC, "Close requested"),
  ASYNC_DELETE_REQUESTED(LEVEL.TRACE, STORAGE_CATEGORY.ASYNC, "Delete requested"),
  ASYNC_ADD_REQUESTED(LEVEL.TRACE, STORAGE_CATEGORY.ASYNC, "Add item requested for key {}"),
  ASYNC_PAUSE_REQUESTED(LEVEL.TRACE, STORAGE_CATEGORY.ASYNC, "Pause storage layer requested"),
  ASYNC_STATISTICS_REQUESTED(LEVEL.TRACE, STORAGE_CATEGORY.ASYNC, "Storage statistics requested"),
  ASYNC_IS_EMPTY_REQUESTED(LEVEL.TRACE, STORAGE_CATEGORY.ASYNC, "Is empty requested"),
  ASYNC_LAST_KEY_REQUESTED(LEVEL.TRACE, STORAGE_CATEGORY.ASYNC, "Last Key requested"),
  ASYNC_SIZE_REQUESTED(LEVEL.TRACE, STORAGE_CATEGORY.ASYNC, "Size requested"),
  ASYNC_GET_REQUESTED(LEVEL.TRACE, STORAGE_CATEGORY.ASYNC, "Get for key {} requested"),
  ASYNC_REMOVE_REQUESTED(LEVEL.TRACE, STORAGE_CATEGORY.ASYNC, "Remove for key {} requested"),
  ASYNC_CLOSE_COMPLETED(LEVEL.TRACE, STORAGE_CATEGORY.ASYNC, "Close request has completed"),
  ASYNC_CLOSE_FAILED(LEVEL.ERROR, STORAGE_CATEGORY.ASYNC, "Close request has raised an exception"),
  ASYNC_ENABLE_AUTO_PAUSE(LEVEL.TRACE, STORAGE_CATEGORY.ASYNC, "Async Auto Pause has been set to {} milliseconds"),
  ASYNC_REQUEST_ON_CLOSED_STORE(LEVEL.ERROR, STORAGE_CATEGORY.ASYNC, "A request has been made on a closed store"),
  //endregion

  //region StoreFactoryFactory
  FOUND_FACTORY(LEVEL.TRACE, STORAGE_CATEGORY.FACTORY, "Found matching factory {}"),
  FOUND_CONSTRUCTOR(LEVEL.TRACE,STORAGE_CATEGORY.FACTORY, "Found suitable constructor for {}"),
  NO_CONSTRUCTOR_FOUND(LEVEL.ERROR,STORAGE_CATEGORY.FACTORY, "No suitable constructor found for {}"),
  NO_MATCHING_FACTORY(LEVEL.ERROR,STORAGE_CATEGORY.FACTORY,"No matching storage factory found for {}"),

  FOUND_CACHE_FACTORY(LEVEL.TRACE, STORAGE_CATEGORY.FACTORY, "Found matching factory for cache implementation {}"),
  FOUND_CACHE_CONSTRUCTOR(LEVEL.TRACE,STORAGE_CATEGORY.FACTORY, "Found suitable constructor for cache for {}"),
  CREATED_NEW_CACHE_INSTANCE(LEVEL.TRACE,STORAGE_CATEGORY.FACTORY, "Created new instance of cache {}"),
  NO_CACHE_FOUND(LEVEL.ERROR,STORAGE_CATEGORY.FACTORY,"No matching cache factory found for {}"),
  //endregion

  //region BaseExpiredHandler
  REMOVING_EXPIRED_ENTRY(LEVEL.TRACE, STORAGE_CATEGORY.FACTORY, "Removing expired entry, {}, from {}"),
  //endregion

  // region S3
  S3_ARCHIVING_DATA (LEVEL.INFO, STORAGE_CATEGORY.FILE, "Successfully archived {} to s3 bucket {}"),
  S3_RESTORED_DATA (LEVEL.INFO, STORAGE_CATEGORY.FILE, "Successfully restored {} from s3 bucket {}"),
  S3_ENTITY_DELETED (LEVEL.INFO, STORAGE_CATEGORY.FILE, "Successfully deleted {} from s3 bucket {}"),
  S3_MD5_HASH_FAILED (LEVEL.ERROR, STORAGE_CATEGORY.FILE, "MD5 hash on {} failed, expected {}, computed {}"),
  S3_FILE_DELETE_FAILED (LEVEL.ERROR, STORAGE_CATEGORY.FILE, "Unable to delete {}, unable to restore data from S3"),
  // endregion

  // region S3
  ARCHIVE_MONITOR_FAILED (LEVEL.ERROR, STORAGE_CATEGORY.FILE, "Exception raised during archival of store {}"),
  // endregion

  //region StoreBuilder
  STORAGE_ALREADY_CONFIGURED(LEVEL.ERROR, STORAGE_CATEGORY.FACTORY, "The storage type has already been configured"),
  NO_SUCH_STORAGE_FOUND(LEVEL.ERROR, STORAGE_CATEGORY.FACTORY, "No such Storage implementation found {}"),
  NO_STORAGE_FACTORY_FOUND(LEVEL.ERROR, STORAGE_CATEGORY.FACTORY, "The storage factory for the type can not be found"),
  BUILT_STORAGE(LEVEL.INFO, STORAGE_CATEGORY.FACTORY, "Successfully built the storage layer {}"),

  FILE_HELPER_FILE_DOES_NOT_EXIST(LEVEL.WARN, STORAGE_CATEGORY.FILE, "The file does not exist, unable to delete it {}"),
  FILE_HELPER_DELETED_FILE(LEVEL.INFO, STORAGE_CATEGORY.FILE, "Deleted file {}"),
  FILE_HELPER_EXCEPTION_RAISED(LEVEL.INFO, STORAGE_CATEGORY.FILE, "Exception raised while deleting {}"),

  INDEX_STORAGE_RELOAD_ERROR(LEVEL.FATAL, STORAGE_CATEGORY.FILE, "Failed to read header : {} expected {}"),
  INDEX_STORAGE_RESUME_ERROR(LEVEL.FATAL, STORAGE_CATEGORY.FILE, "Failed to resume store while suspended {}"),
  INDEX_STORAGE_RELOAD_STATE(LEVEL.FATAL, STORAGE_CATEGORY.FILE, "{}"),

  DEBUG_LOGGING(LEVEL.DEBUG, STORAGE_CATEGORY.MONITOR, "{}"),
  DEBUG_THREAD_MONITOR_LOGGING(LEVEL.FATAL, STORAGE_CATEGORY.MONITOR, "{}"),

  CACHE_ALREADY_CONFIGURED(LEVEL.WARN, STORAGE_CATEGORY.FACTORY, "The cache has already been configured"),
  NO_SUCH_CACHE_FOUND(LEVEL.ERROR, STORAGE_CATEGORY.FACTORY, "No such cache implementation found {}"),
  DEFAULTING_CACHE(LEVEL.INFO, STORAGE_CATEGORY.FACTORY, "Defaulting the cache implementation to {}");

  //endregion

  private final @Getter String message;
  private final @Getter LEVEL level;
  private final @Getter Category category;
  private final @Getter int parameterCount;

  StorageLogMessages(LEVEL level, STORAGE_CATEGORY category, String message) {
    this.message = message;
    this.level = level;
    this.category = category;
    int location = message.indexOf("{}");
    int count = 0;
    while (location != -1) {
      count++;
      location = message.indexOf("{}", location + 2);
    }
    this.parameterCount = count;
  }

  public enum STORAGE_CATEGORY implements Category {
    FILE("File"),
    MEMORY("Memory"),
    TIER("Tier"),
    CACHE("Cache"),
    ASYNC("Async"),
    MONITOR("Monitor"),
    FACTORY("Factory");

    private final @Getter String description;

    public String getDivision(){
      return "Storage";
    }

    STORAGE_CATEGORY(String description) {
      this.description = description;
    }
  }
}
