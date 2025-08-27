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

import lombok.Getter;

import java.util.Date;

public class StorageStatistics implements Statistics {

  @Getter
  private final long reads;
  @Getter
  private final long writes;
  @Getter
  private final long deletes;
  @Getter
  private final long bytesRead;
  @Getter
  private final long bytesWritten;
  @Getter
  private final long writeLatency;
  @Getter
  private final long readLatency;
  @Getter
  private final long totalSize;
  @Getter
  private final long totalEmptySpace;
  @Getter
  private final int partitionCount;

  public StorageStatistics(long reads, long writes, long deletes) {
    this.reads = reads;
    this.writes = writes;
    this.deletes = deletes;
    this.bytesRead = 0;
    this.bytesWritten = 0;
    this.totalSize = 0;
    this.totalEmptySpace = 0;
    this.partitionCount = 0;
    this.readLatency = 0;
    this.writeLatency = 0;
  }

  @SuppressWarnings("java:S107")
  public StorageStatistics(long reads, long writes, long deletes, long bytesRead, long bytesWritten, long readTime, long writeTime, long totalSize, long totalEmptySpace,
      int partitionCount) {
    this.reads = reads;
    this.writes = writes;
    this.deletes = deletes;
    this.bytesRead = bytesRead;
    this.bytesWritten = bytesWritten;
    this.totalSize = totalSize;
    this.totalEmptySpace = totalEmptySpace;
    this.partitionCount = partitionCount;

    if (reads != 0) {
      this.readLatency = readTime / reads;
    } else {
      this.readLatency = 0;
    }
    if (writes != 0) {
      this.writeLatency = writeTime / writes;
    } else {
      this.writeLatency = 0;
    }
  }

  public long getIops() {
    return reads + writes;
  }

  @Override
  public String toString() {
    Date dt = new Date();
    StringBuilder sb = new StringBuilder(dt.toString()).append(",\t");
    sb.append("Reads:");
    sb.append(getReads());
    sb.append(",\t");
    sb.append("Writes:");
    sb.append(getWrites());
    sb.append(",\t");
    sb.append("Deletes:");
    sb.append(getDeletes());
    sb.append(",\t");
    sb.append("IOPS:");
    sb.append(getIops());
    sb.append(",\t");
    sb.append("Bytes Read:");
    sb.append(getBytesRead());
    sb.append(",\t");
    sb.append("Bytes Written:");
    sb.append(getBytesWritten());
    sb.append(",\t");
    sb.append("File Size:");
    sb.append(getTotalSize());
    sb.append(",\t");
    sb.append("Empty Space:");
    sb.append(getTotalEmptySpace());
    sb.append(",\t");
    sb.append("File Count:");
    sb.append(getPartitionCount());
    sb.append(",\t");
    sb.append("Read Latency:");
    sb.append(getReadLatency());
    sb.append(",\t");
    sb.append("Write Latency:");
    sb.append(getWriteLatency());

    return sb.toString();
  }
}
