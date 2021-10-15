package io.mapsmessaging.storage.impl.tier.memory;

import io.mapsmessaging.storage.Statistics;
import lombok.Getter;

public class MemoryTierStatistics implements Statistics {

  private @Getter final Statistics memoryStatistics;
  private @Getter final Statistics fileStatistics;

  MemoryTierStatistics(Statistics memoryStatistics, Statistics fileStatistics){
    this.memoryStatistics = memoryStatistics;
    this.fileStatistics = fileStatistics;
  }

}
