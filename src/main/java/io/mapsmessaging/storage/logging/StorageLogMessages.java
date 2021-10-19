package io.mapsmessaging.storage.logging;

import io.mapsmessaging.logging.Category;
import io.mapsmessaging.logging.LEVEL;
import io.mapsmessaging.logging.LogMessage;
import lombok.Getter;

public enum StorageLogMessages implements LogMessage {

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
  //

  //region StoreBuilder
  STORAGE_ALREADY_CONFIGURED(LEVEL.ERROR, STORAGE_CATEGORY.FACTORY, "The storage type has already been configured"),
  NO_SUCH_STORAGE_FOUND(LEVEL.ERROR, STORAGE_CATEGORY.FACTORY, "No such Storage implementation found {}"),
  NO_STORAGE_FACTORY_FOUND(LEVEL.ERROR, STORAGE_CATEGORY.FACTORY, "The storage factory for the type can not be found"),
  BUILT_STORAGE(LEVEL.ERROR, STORAGE_CATEGORY.FACTORY, "Successfully built the storage layer {}"),

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
    FACTORY("Factory");

    private final @Getter String description;

    STORAGE_CATEGORY(String description) {
      this.description = description;
    }
  }
}
