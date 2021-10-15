package io.mapsmessaging.storage.impl.tier.memory;

import io.mapsmessaging.storage.Statistics;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.storage.impl.file.TaskQueue;
import io.mapsmessaging.storage.impl.tier.memory.tasks.TierMigrationTask;
import io.mapsmessaging.utilities.collections.NaturalOrderedLongList;
import io.mapsmessaging.utilities.threads.tasks.TaskScheduler;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class MemoryTierStorage<T extends Storable> implements Storage<T> {

  private final Storage<ObjectMonitor<T>> primary;
  private final Storage<T> secondary;
  private final long scanInterval;
  private final long migrationTime;

  private ScheduledFuture<?> scanner;

  private long lastKey;

  public MemoryTierStorage(Storage<ObjectMonitor<T>> primary, Storage<T> secondary, long scanInterval, long migrationTime) {
    this.primary = primary;
    this.secondary = secondary;
    lastKey = secondary.getLastKey();
    this.migrationTime =migrationTime;
    this.scanInterval = scanInterval;
  }

  @Override
  public void close() throws IOException {
    if (scanner != null) {
      scanner.cancel(false);
    }
    for (Long key : primary.getKeys()) {
      ObjectMonitor<T> monitor = primary.get(key);
      if (monitor != null) {
        secondary.add(monitor.getStorable());
      }
    }
    primary.close();
    secondary.close();
  }

  @Override
  public void delete() throws IOException {
    primary.delete();
    secondary.delete();
  }

  @Override
  public String getName() {
    return primary.getName();
  }

  @Override
  public long size() throws IOException {
    return primary.size() + secondary.size();
  }

  @Override
  public long getLastKey() {
    return lastKey;
  }

  @Override
  public long getLastAccess() {
    return primary.getLastAccess();
  }

  @Override
  public boolean isEmpty() {
    return primary.isEmpty() && secondary.isEmpty();
  }

  @Override
  public TaskQueue getTaskScheduler() {
    return primary.getTaskScheduler();
  }

  @Override
  public @NotNull List<Long> keepOnly(@NotNull List<Long> listToKeep) throws IOException {
    List<Long> next = primary.keepOnly(listToKeep);
    secondary.keepOnly(next);
    return next;
  }

  @Override
  public @NotNull Statistics getStatistics() {
    return new MemoryTierStatistics(primary.getStatistics(), secondary.getStatistics());
  }

  @Override
  public void add(@NotNull T object) throws IOException {
    lastKey = object.getKey();
    primary.add(new ObjectMonitor<>(object));
  }

  @Override
  public boolean remove(long key) throws IOException {
    return primary.remove(key) || secondary.remove(key);

  }

  @Override
  public @Nullable T get(long key) throws IOException {
    ObjectMonitor<T> monitor = primary.get(key);
    if (monitor != null) {
      monitor.setLastAccess(System.currentTimeMillis());
      return monitor.getStorable();
    }
    return secondary.get(key);
  }

  @Override
  public @NotNull List<Long> getKeys() {
    List<Long> keyList = new NaturalOrderedLongList();
    keyList.addAll(primary.getKeys());
    keyList.addAll(secondary.getKeys());
    return keyList;
  }

  @Override
  public void setExecutor(TaskScheduler executor) {
    primary.setExecutor(executor);
    secondary.setExecutor(executor);
    scanner = primary.getTaskScheduler().scheduleAtFixedRate(new TierMigrationTask(this), scanInterval, TimeUnit.MILLISECONDS);
  }


  @SneakyThrows
  public void scan() {
    long test = System.currentTimeMillis() + migrationTime;
    for (Long key : primary.getKeys()) {
      ObjectMonitor<T> check = primary.get(key);
      if (check != null && check.getLastAccess() < test) {
        secondary.add(check.getStorable());
        primary.remove(key);
      }
    }
  }
}
