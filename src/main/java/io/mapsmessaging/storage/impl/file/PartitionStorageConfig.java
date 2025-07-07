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

package io.mapsmessaging.storage.impl.file;

import io.mapsmessaging.storage.ExpiredStorableHandler;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.StorableFactory;
import lombok.Data;

@Data
public class PartitionStorageConfig<T extends Storable> {

  private String fileName;
  private StorableFactory<T> storableFactory;
  private ExpiredStorableHandler expiredHandler;
  private boolean sync;
  private int itemCount;
  private int capacity;
  private long maxPartitionSize;
  private int expiredEventPoll;
  private TaskQueue taskQueue;
  private String archiveName = "None";
  private long archiveIdleTime = -1;
  private boolean s3Compression;
  private String s3AccessKeyId;
  private String s3SecretAccessKey;
  private String s3RegionName;
  private String s3BucketName;
  private String migrationDestination;
  private String digestName = "";

  public PartitionStorageConfig(){
    // Default constructor
  }


  public PartitionStorageConfig(PartitionStorageConfig<T> lhs){
    this.fileName = lhs.getFileName();
    this.storableFactory = lhs.storableFactory;
    this.sync = lhs.isSync();
    this.capacity = lhs.getCapacity();
    this.expiredHandler = lhs.expiredHandler;
    this.itemCount = lhs.itemCount;
    this.maxPartitionSize = lhs.maxPartitionSize;
    this.expiredEventPoll = lhs.expiredEventPoll;
    this.taskQueue = lhs.taskQueue;
    this.archiveName = lhs.archiveName;
    this.archiveIdleTime = lhs.archiveIdleTime;
    this.s3Compression = lhs.s3Compression;
    this.s3AccessKeyId = lhs.s3AccessKeyId;
    this.s3SecretAccessKey = lhs.s3SecretAccessKey;
    this.s3RegionName = lhs.s3RegionName;
    this.s3BucketName = lhs.s3BucketName;
    this.migrationDestination = lhs.migrationDestination;
    this.digestName = lhs.digestName;
  }
}
