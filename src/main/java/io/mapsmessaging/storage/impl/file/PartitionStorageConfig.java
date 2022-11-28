/*
 *   Copyright [2020 - 2022]   [Matthew Buckton]
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 */

package io.mapsmessaging.storage.impl.file;

import io.mapsmessaging.storage.ExpiredStorableHandler;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.StorableFactory;
import lombok.Getter;
import lombok.Setter;

public class PartitionStorageConfig<T extends Storable> {

  @Getter
  @Setter
  private String fileName;

  @Getter
  @Setter
  private StorableFactory<T> storableFactory;

  @Getter
  @Setter
  private ExpiredStorableHandler expiredHandler;

  @Getter
  @Setter
  private boolean sync;

  @Getter
  @Setter
  private int itemCount;

  @Getter
  @Setter
  private long maxPartitionSize;

  @Getter
  @Setter
  private int expiredEventPoll;

  @Getter
  @Setter
  private TaskQueue taskQueue;

  @Getter
  @Setter
  private String archiveName = "None";

  @Getter
  @Setter
  private String s3AccessKeyId;

  @Getter
  @Setter
  private String s3SecretAccessKey;

  @Getter
  @Setter
  private String s3RegionName;

  @Getter
  @Setter
  private String s3BucketName;

  @Getter
  @Setter
  private long archiveIdleTime = -1;
}
