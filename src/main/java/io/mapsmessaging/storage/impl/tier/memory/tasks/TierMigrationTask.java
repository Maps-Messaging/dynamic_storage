package io.mapsmessaging.storage.impl.tier.memory.tasks;

import io.mapsmessaging.storage.impl.file.tasks.FileTask;
import io.mapsmessaging.storage.impl.tier.memory.MemoryTierStorage;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;

public class TierMigrationTask implements FileTask<Void>, Runnable {

  private final MemoryTierStorage<?> storage;

  public TierMigrationTask(@NotNull MemoryTierStorage<?> storage) {
    this.storage = storage;
  }

  @SneakyThrows
  @Override
  public void run() {
    storage.getTaskScheduler().submit(this);
  }

  @Override
  public Void call() {
    storage.scan();
    return null;
  }
}
