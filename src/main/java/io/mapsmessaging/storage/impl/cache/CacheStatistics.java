package io.mapsmessaging.storage.impl.cache;

import io.mapsmessaging.storage.Statistics;
import lombok.Getter;

public class CacheStatistics implements Statistics {

  private final @Getter
  long miss;
  private final @Getter
  long hit;
  private final @Getter
  int size;
  private final @Getter
  Statistics storageStatistics;

  public CacheStatistics(long miss, long hit, int size, Statistics storageStatistics) {
    this.miss = miss;
    this.hit = hit;
    this.size = size;
    this.storageStatistics = storageStatistics;
  }

  @Override
  public String toString() {
    return getStorageStatistics().toString() + ",\tCache Hits:" + getHit() + ",\t Cache Miss:" + getHit() + ",\t Cache Size:" + getSize();
  }
}
