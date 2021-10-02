package io.mapsmessaging.storage.impl.cache;

import io.mapsmessaging.storage.Statistics;
import lombok.Getter;

public class CacheStatistics implements Statistics {

  private final @Getter long miss;
  private final @Getter long hit;
  private final @Getter int size;
  private final @Getter Statistics partitionStatistics;

  public CacheStatistics(long miss, long hit, int size, Statistics partitionStatistics){
    this.miss = miss;
    this.hit = hit;
    this.size = size;
    this.partitionStatistics = partitionStatistics;
  }

}
