package io.mapsmessaging.storage;

import lombok.Getter;

public class StorageStatistics extends Statistics {

  private final @Getter long reads;
  private final @Getter long writes;
  private final @Getter long deletes;
  private final @Getter long bytesRead;
  private final @Getter long bytesWritten;
  private final @Getter long writeLatency;
  private final @Getter long readLatency;

  public StorageStatistics(long reads, long writes, long deletes, long bytesRead, long bytesWritten, long readTime, long writeTime){
    this.reads = reads;
    this.writes = writes;
    this.deletes = deletes;
    this.bytesRead = bytesRead;
    this.bytesWritten = bytesWritten;
    if(reads != 0) {
      this.readLatency = readTime/reads;
    }
    else{
      this.readLatency = 0;
    }
    if(writes != 0) {
      this.writeLatency = writeTime/writes;
    }
    else{
      this.writeLatency = 0;
    }
  }

  public long getIops(){
    return reads + writes;
  }
}
