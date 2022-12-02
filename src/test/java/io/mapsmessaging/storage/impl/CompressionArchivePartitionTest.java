package io.mapsmessaging.storage.impl;

import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.storage.impl.file.PartitionStorage;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class CompressionArchivePartitionTest  extends BaseTest {
  @Test
  void compressArchiveAndRestorePartition() throws IOException, InterruptedException {
    Map<String, String> properties = BasePartitionStoreTest.buildProperties(false);
    properties.put("archiveName", "Compress");
    properties.put("archiveIdleTime", ""+ TimeUnit.SECONDS.toMillis(30));
    Storage<MappedData> storage = BasePartitionStoreTest.build(properties, testName);
    for (int x = 0; x < 1100; x++) {
      MappedData message = createMessageBuilder(x);
      storage.add(message);
    }

    // We should have exceeded the partition limits and have 10 partitions, lets wait the time out period
    TimeUnit.SECONDS.sleep(40);
    ((PartitionStorage<MappedData>)storage).scanForArchiveMigration();

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
    properties.put("archiveIdleTime", ""+TimeUnit.SECONDS.toMillis(30));
    Storage<MappedData> storage = BasePartitionStoreTest.build(properties, testName);
    for (int x = 0; x < 1100; x++) {
      MappedData message = createMessageBuilder(x);
      storage.add(message);
    }

    // We should have exceeded the partition limits and have 10 partitions, lets wait the time out period
    TimeUnit.SECONDS.sleep(40);
    ((PartitionStorage<MappedData>)storage).scanForArchiveMigration();
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
