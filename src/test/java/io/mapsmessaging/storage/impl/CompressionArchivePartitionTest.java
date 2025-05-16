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

package io.mapsmessaging.storage.impl;

import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.storage.TierMigrationMonitor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

class CompressionArchivePartitionTest  extends BaseTest {


  @ParameterizedTest
  @ValueSource(strings = {"None", "MD5", "SHA-1", "SHA-256"})
  void compressArchiveAndRestorePartition(String digestName) throws IOException, InterruptedException {
    Map<String, String> properties = BasePartitionStoreTest.buildProperties(false);
    properties.put("archiveName", "Compress");
    properties.put("archiveIdleTime", ""+ TimeUnit.SECONDS.toMillis(4));
    properties.put("digestName", digestName);
    Storage<MappedData> storage = BasePartitionStoreTest.build(properties, testName);
    for (int x = 0; x < 1100; x++) {
      MappedData message = createMessageBuilder(x);
      storage.add(message);
    }

    // We should have exceeded the partition limits and have 10 partitions, lets wait the time out period
    TimeUnit.SECONDS.sleep(5);

    ((TierMigrationMonitor)storage).scanForArchiveMigration();

    // They should now be archived
    for (int x = 0; x < 1100; x++) {
      MappedData data = storage.get(x);
      Assertions.assertNotNull(data, "Expected data for key "+x);
      Assertions.assertEquals(data.key, x);

    }
    storage.delete();
  }

  @Test
  void compressArchiveAndDeleteStore() throws IOException, InterruptedException {
    Map<String, String> properties = BasePartitionStoreTest.buildProperties(false);
    properties.put("archiveName", "Compress");
    properties.put("archiveIdleTime", ""+TimeUnit.SECONDS.toMillis(4));
    Storage<MappedData> storage = BasePartitionStoreTest.build(properties, testName);
    for (int x = 0; x < 1100; x++) {
      MappedData message = createMessageBuilder(x);
      storage.add(message);
    }

    // We should have exceeded the partition limits and have 10 partitions, lets wait the time out period
    TimeUnit.SECONDS.sleep(5);
    ((TierMigrationMonitor)storage).scanForArchiveMigration();
    File file = new File("test_file" + File.separator+testName);
    // We should have 10 zip files
    int count =0;
    File[] files = file.listFiles();
    for(File children:files){
      if(children.getName().endsWith("_zip")){
        count++;
      }
    }
    Assertions.assertEquals(10, count, "Expected 10 compressed files");
    storage.delete();
  }

}
