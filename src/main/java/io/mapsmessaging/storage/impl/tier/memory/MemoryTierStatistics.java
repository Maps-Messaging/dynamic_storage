package io.mapsmessaging.storage.impl.tier.memory;

import io.mapsmessaging.storage.Statistics;
import lombok.Getter;

public class MemoryTierStatistics implements Statistics {

  private final @Getter Statistics memoryStatistics;
  private final @Getter Statistics fileStatistics;
  private final @Getter long migratedCount;

  MemoryTierStatistics(Statistics memoryStatistics, Statistics fileStatistics, long migratedCount){
    this.memoryStatistics = memoryStatistics;
    this.fileStatistics = fileStatistics;
    this.migratedCount = migratedCount;
  }

}
