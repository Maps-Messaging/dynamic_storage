/*
 *
 *  Copyright [ 2020 - 2024 ] Matthew Buckton
 *  Copyright [ 2024 - 2026 ] MapsMessaging B.V.
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

import io.mapsmessaging.storage.*;
import io.mapsmessaging.storage.impl.file.FileHelper;
import io.mapsmessaging.storage.impl.file.partition.IndexRecord;
import io.mapsmessaging.utilities.threads.tasks.ThreadLocalContext;
import io.mapsmessaging.utilities.threads.tasks.ThreadStateContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

class PartitionStoreTest extends BasePartitionStoreTest {


  private Storage<MappedData> createCompactionStore() throws IOException {
    File file = new File("test_file" + File.separator + "testIndexCompaction");
    Files.deleteIfExists(file.toPath());

    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("storeType", "Partition");
    properties.put("Sync", "" + false);
    properties.put("ItemCount", ""+ 1_000_000);
    properties.put("MaxPartitionSize", "" + (1024L * 1024L)); // set to 1MB data limit // force the index
    StorageBuilder<MappedData> storageBuilder = new StorageBuilder<>();
    storageBuilder
        .setFactory(getFactory())
        .setName("test_file" + File.separator + "testIndexCompaction")
        .setProperties(properties);
    return storageBuilder.build();
  }

  @Test
  void testCompactionWithTrailingDeletion() throws IOException {
    int eventCount = 10_000;
    int skipCount = 100;
    Storage<MappedData> storage = null;
    try {
      storage = createCompactionStore();
      ThreadStateContext context = new ThreadStateContext();
      context.add("domain", "ResourceAccessKey");
      ThreadLocalContext.set(context);
      // Remove any before we start

      int deleteIndex = 0;
      for (int x = 0; x < eventCount; x++) {
        MappedData message = createMessageBuilder(x);
        validateMessage(message, x);
        storage.add(message);
        if (storage.size() > skipCount) {
          Assertions.assertTrue(storage.remove(deleteIndex), "Failed to delete index " + deleteIndex);
          deleteIndex++;
          if (deleteIndex % skipCount == 0) {
            deleteIndex++; // skip every 500
          }
        }
      }
      while (deleteIndex < eventCount) {
        Assertions.assertTrue(storage.remove(deleteIndex), "Failed to delete index " + deleteIndex);
        deleteIndex++;
        if (deleteIndex % skipCount == 0) {
          deleteIndex++; // skip every 500
        }
      }

      Assertions.assertEquals(skipCount-1, storage.size());

      for (int x = skipCount; x < eventCount; x = x + skipCount) {
        Assertions.assertTrue(storage.contains(x), "Should contain index: " + x);
      }
      long index = skipCount;
      List<Long> keyList = storage.getKeys();
      for (Long key : keyList) {
        Assertions.assertEquals(index, key);
        index += skipCount;
      }
      storage.keepOnly(new ArrayList<>());

      Assertions.assertTrue(storage.isEmpty());
    } finally {
      if (storage != null) {
        storage.delete();
      }
    }
  }

  @Test
  void testIndexCompaction() throws IOException, ExecutionException, InterruptedException {
    AsyncStorage<MappedData> storage = new AsyncStorage<>(createCompactionStore());

    ThreadStateContext context = new ThreadStateContext();
    context.add("domain", "ResourceAccessKey");
    ThreadLocalContext.set(context);
    // Remove any before we start

    try {
      for (int x = 0; x < 10_000; x++) {
        MappedData message = createMessageBuilder(x);
        storage.add(message, null).get();
        Assertions.assertEquals(x+1, storage.size().get());
        MappedData lookup = storage.get(x).get();
        Assertions.assertNotNull(lookup);
        Assertions.assertEquals(message.key, lookup.key);
      }
      // We should have compacted the index and have multiple indexes now
      Statistics statistics = storage.getStatistics().get();
      Assertions.assertEquals(26, ((StorageStatistics) statistics).getPartitionCount());
      List<Long> keys = storage.getKeys().get();
      long index =0;
      for(Long key:keys){
        Assertions.assertEquals(index, key);
        index++;
      }
      Assertions.assertEquals(10_000, keys.size());

    } finally {
      storage.delete().get();
    }
  }


  @Test
  void testRestart() throws IOException, ExecutionException, InterruptedException {
    File file = new File("test_file" + File.separator + "testRestart");
    Files.deleteIfExists(file.toPath());

    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("storeType", "Partition");
    properties.put("Sync", "" + false);
    properties.put("ItemCount", ""+ 1_000);
    properties.put("ExpiredEventPoll", ""+120);
    properties.put("MaxPartitionSize", "" + (1024L * 1024L)); // set to 1MB data limit // force the index
    StorageBuilder<MappedData> storageBuilder = new StorageBuilder<>();
    storageBuilder
        .setFactory(getFactory())
        .setName("test_file" + File.separator + "testRestart")
        .setProperties(properties);
    AsyncStorage<MappedData> storage = new AsyncStorage<>(storageBuilder.build());

    ThreadStateContext context = new ThreadStateContext();
    context.add("domain", "ResourceAccessKey");
    ThreadLocalContext.set(context);
    // Remove any before we start

    try {
      for (int x = 0; x < 10_000; x++) {
        MappedData message = createMessageBuilder(x);
        message.setExpiry(System.currentTimeMillis()+3000); // 3 Seconds
        message.setKey(x);
        storage.add(message, null).get();
        Assertions.assertEquals(x+1, storage.size().get());
        MappedData lookup = storage.get(x).get();
        Assertions.assertNotNull(lookup);
        Assertions.assertEquals(message.key, lookup.key);
      }
      storage.close();
      TimeUnit.SECONDS.sleep(4);
      // Now let's reopen the file and check the expired events
      storage = new AsyncStorage<>(storageBuilder.build());
      int count = 0;
      while(storage.size().get() != 0 && count < 20){
        TimeUnit.SECONDS.sleep(1);
        count++;
      }
      Assertions.assertEquals(0, (long) storage.size().get());

    } finally {
      storage.delete().get();
    }
  }

  @Test
  void fileStorageCapacityEvictionTest() throws IOException, InterruptedException {
    File file = new File("test_file" + File.separator + "capacityEvictionTest");
    Files.deleteIfExists(file.toPath());

    AtomicLong expired = new AtomicLong();
    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("Sync", "false");
    properties.put("Capacity", "10"); // enforce max retained messages
    properties.put("ItemCount", "1000"); // optional: avoid premature rollover
    properties.put("MaxPartitionSize", String.valueOf(1024 * 1024)); // avoid rollover side effects
    properties.put("ExpiredEventPoll", "1");
    properties.put("storeType", "Partition");

    StorageBuilder<MappedData> storageBuilder = new StorageBuilder<>();
    storageBuilder
        .setFactory(getFactory())
        .setName(file.getPath())
        .setExpiredHandler(listOfExpiredEntries -> expired.incrementAndGet())
        .setProperties(properties);

    Storage<MappedData> storage = storageBuilder.build();

    try {
      // Add more than capacity
      for (int i = 0; i < 25; i++) {
        storage.add(createMessageBuilder(i));
      }
      int count = 0;
      while(count < 30 && storage.size() == 25){
        Thread.sleep(100);
        count++;
      }


      Assertions.assertEquals(15, expired.get());

      // Should retain only the last 10 items
      Assertions.assertEquals(10, storage.size(), "Storage should retain only the most recent 10 messages");

      for (int i = 0; i < 15; i++) {
        Assertions.assertTrue(storage.get(0) == null);
        Assertions.assertFalse(storage.contains(i), "Key " + i + " should have been evicted");
      }

      for (int i = 15; i < 25; i++) {
        Assertions.assertTrue(storage.contains(i), "Key " + i + " should still be present");
      }

    } finally {
      storage.delete();
    }
  }

  @Test
  void reopenedStoreCreatesNextPartitionWithoutReusingExistingPartitionFile() throws IOException {
    File file = new File("test_file" + File.separator + "reopenPartitionRollover");
    if (file.exists()) {
      FileHelper.delete(file, true);
    }

    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("storeType", "Partition");
    properties.put("Sync", "" + false);
    properties.put("ItemCount", "" + 10);
    properties.put("ExpiredEventPoll", "" + 120);
    properties.put("MaxPartitionSize", "" + (1024L * 1024L));

    StorageBuilder<MappedData> storageBuilder = new StorageBuilder<>();
    storageBuilder
        .setFactory(getFactory())
        .setName(file.getPath())
        .setProperties(properties);

    Storage<MappedData> storage = null;
    try {
      storage = storageBuilder.build();

      for (int x = 0; x < 20; x++) {
        storage.add(createMessageBuilder(x));
      }

      Assertions.assertEquals(20, storage.size());
      storage.close();
      storage = null;

      storage = storageBuilder.build();
      storage.add(createMessageBuilder(20));

      Assertions.assertEquals(21, storage.size());

      for (int x = 0; x <= 20; x++) {
        MappedData data = storage.get(x);
        Assertions.assertNotNull(data, "Expected data for key " + x);
        Assertions.assertEquals(x, data.key);
      }

      Assertions.assertTrue(new File(file, "partition_0_index").exists());
      Assertions.assertTrue(new File(file, "partition_1_index").exists());
      Assertions.assertTrue(new File(file, "partition_2_index").exists());
    } finally {
      if (storage != null) {
        storage.delete();
      } else if (file.exists()) {
        FileHelper.delete(file, true);
      }
    }
  }

  @Test
  void deleteRemovesUnknownChildrenUnderStorageRootWithoutTouchingWorkingDirectory() throws IOException {
    File file = new File("test_file" + File.separator + "deleteWithOrphanChild");
    File workingDirectoryFile = new File("orphan.tmp");
    if(file.exists()) {
      FileHelper.delete(file, true);
    }
    if(workingDirectoryFile.exists()) {
      Files.deleteIfExists(workingDirectoryFile.toPath());
    }

    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("storeType", "Partition");
    properties.put("Sync", "" + false);
    properties.put("ItemCount", "" + 10);
    properties.put("ExpiredEventPoll", "" + 120);
    properties.put("MaxPartitionSize", "" + (1024L * 1024L));

    StorageBuilder<MappedData> storageBuilder = new StorageBuilder<>();
    storageBuilder
        .setFactory(getFactory())
        .setName(file.getPath())
        .setProperties(properties);

    Storage<MappedData> storage = null;
    try {
      storage = storageBuilder.build();
      storage.add(createMessageBuilder(1));

      File orphanFile = new File(file, "orphan.tmp");
      Files.writeString(orphanFile.toPath(), "orphan");
      Files.writeString(workingDirectoryFile.toPath(), "do-not-delete");

      storage.delete();
      storage = null;

      Assertions.assertFalse(file.exists(), "Storage root should have been deleted");
      Assertions.assertTrue(workingDirectoryFile.exists(), "Working directory file must not be touched");
    } finally {
      if (storage != null) {
        storage.delete();
      }
      if(file.exists()) {
        FileHelper.delete(file, true);
      }
      if(workingDirectoryFile.exists()) {
        Files.deleteIfExists(workingDirectoryFile.toPath());
      }
    }
  }

  @Test
  void failedAddDoesNotLeaveFailedObjectVisibleFromCache() throws IOException {
    File file = new File("test_file" + File.separator + "cacheFailedAdd");
    if (file.exists()) {
      FileHelper.delete(file, true);
    }

    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("storeType", "Partition");
    properties.put("Sync", "false");
    properties.put("ItemCount", "10");
    properties.put("ExpiredEventPoll", "120");
    properties.put("MaxPartitionSize", "" + (1024L * 1024L));

    StorageBuilder<MappedData> storageBuilder = new StorageBuilder<>();
    storageBuilder
        .setFactory(getFactory())
        .setCache()
        .setName(file.getPath())
        .setProperties(properties);

    Storage<MappedData> storage = null;
    try {
      storage = storageBuilder.build();

      MappedData original = createMessageBuilder(1);
      original.setMap(new LinkedHashMap<>(original.getMap()));
      original.getMap().put("cache-test-marker", "original");
      storage.add(original);

      MappedData failedReplacement = createMessageBuilder(1);
      failedReplacement.setMap(new LinkedHashMap<>(failedReplacement.getMap()));
      failedReplacement.getMap().put("cache-test-marker", "failed-replacement");

      final Storage<MappedData> cacheStorage = storage;
      Assertions.assertThrows(IOException.class, () -> cacheStorage.add(failedReplacement));

      MappedData data = storage.get(1);
      Assertions.assertNotNull(data);
      Assertions.assertEquals(1, data.getKey());
      Assertions.assertEquals(
          "original",
          data.getMap().get("cache-test-marker"),
          "Failed replacement must not be visible from cache");
    } finally {
      if (storage != null) {
        storage.delete();
      }
      if (file.exists()) {
        FileHelper.delete(file, true);
      }
    }
  }

  void migrateArchiveAndRestorePartition() throws IOException, InterruptedException {
    Map<String, String> properties = buildProperties(false);
    properties.put("deferredName", "Migrate");
    properties.put("archiveIdleTime", ""+TimeUnit.SECONDS.toMillis(30));
    properties.put("migrationPath", "P:/migration/");
    Storage<MappedData> storage = build(properties,  BasePartitionStoreTest.computeNameFromTestName(testName));
    for (int x = 0; x < 1100; x++) {
      MappedData message = createMessageBuilder(x);
      storage.add(message);
    }

    // We should have exceeded the partition limits and have 10 partitions, lets wait the time out period
    TimeUnit.SECONDS.sleep(40);
    ((TierMigrationMonitor)storage).scanForArchiveMigration();

    // They should now be archived
    for (int x = 0; x < 1100; x++) {
      MappedData data = storage.get(x);
      Assertions.assertNotNull(data, "Expected data for key "+x);
      Assertions.assertEquals(data.key, x);

    }
    storage.delete();
  }

  void migrateArchiveAndDeleteStore() throws IOException, InterruptedException {
    Map<String, String> properties = buildProperties(false);
    properties.put("deferredName", "Migrate");
    properties.put("archiveIdleTime", ""+TimeUnit.SECONDS.toMillis(30));
    properties.put("migrationPath", "P:/migration/");
    Storage<MappedData> storage = build(properties,  BasePartitionStoreTest.computeNameFromTestName(testName));
    for (int x = 0; x < 1100; x++) {
      MappedData message = createMessageBuilder(x);
      storage.add(message);
    }

    // We should have exceeded the partition limits and have 10 partitions, lets wait the time out period
    TimeUnit.SECONDS.sleep(40);
    ((TierMigrationMonitor)storage).scanForArchiveMigration();
    File file = new File("P:/migration/test_file" + File.separator+ BasePartitionStoreTest.computeNameFromTestName(testName));
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

  @Test
  void compressArchiveAndRestorePartition() throws IOException, InterruptedException {
    Map<String, String> properties = buildProperties(false);
    properties.put("deferredName", "Compress");
    properties.put("archiveIdleTime", ""+TimeUnit.SECONDS.toMillis(30));
    Storage<MappedData> storage = build(properties,  BasePartitionStoreTest.computeNameFromTestName(testName));
    for (int x = 0; x < 1100; x++) {
      MappedData message = createMessageBuilder(x);
      storage.add(message);
    }

    // We should have exceeded the partition limits and have 10 partitions, lets wait the time out period
    TimeUnit.SECONDS.sleep(40);
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
    Map<String, String> properties = buildProperties(false);
    properties.put("deferredName", "Compress");
    properties.put("archiveIdleTime", ""+TimeUnit.SECONDS.toMillis(30));
    Storage<MappedData> storage = build(properties,  BasePartitionStoreTest.computeNameFromTestName(testName));
    for (int x = 0; x < 1100; x++) {
      MappedData message = createMessageBuilder(x);
      storage.add(message);
    }

    // We should have exceeded the partition limits and have 10 partitions, lets wait the time out period
    TimeUnit.SECONDS.sleep(40);
    ((TierMigrationMonitor)storage).scanForArchiveMigration();
    File file = new File("test_file" + File.separator+ BasePartitionStoreTest.computeNameFromTestName(testName));
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

  @Test
  void uncleanReopenRemovesIndexEntryForTruncatedDataRecord() throws IOException {
    File file = new File("test_file" + File.separator + "truncatedDataRecovery");
    if (file.exists()) {
      FileHelper.delete(file, true);
    }

    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("storeType", "Partition");
    properties.put("Sync", "false");
    properties.put("ItemCount", "100");
    properties.put("ExpiredEventPoll", "120");
    properties.put("MaxPartitionSize", "" + (1024L * 1024L));

    StorageBuilder<MappedData> storageBuilder = new StorageBuilder<>();
    storageBuilder
        .setFactory(getFactory())
        .setName(file.getPath())
        .setProperties(properties);

    Storage<MappedData> storage = null;
    try {
      storage = storageBuilder.build();

      storage.add(createMessageBuilder(1));
      storage.add(createMessageBuilder(2));

      Assertions.assertEquals(2, storage.size());
      Assertions.assertNotNull(storage.get(1));
      Assertions.assertNotNull(storage.get(2));

      storage.close();
      storage = null;

      File dataFile = new File(file, "partition_0_index_data");
      long originalLength = dataFile.length();
      Assertions.assertTrue(originalLength > 64, "Data file should contain test records");

      try (RandomAccessFile randomAccessFile = new RandomAccessFile(dataFile, "rw")) {
        randomAccessFile.setLength(originalLength - 16);
      }

      markFileOpen(new File(file, "partition_0_index"));
      markFileOpen(dataFile);

      storage = storageBuilder.build();

      Assertions.assertEquals(1, storage.size());
      Assertions.assertTrue(storage.contains(1));
      Assertions.assertFalse(storage.contains(2));

      MappedData first = storage.get(1);
      Assertions.assertNotNull(first);
      Assertions.assertEquals(1, first.getKey());

      Assertions.assertNull(storage.get(2));

      List<Long> keys = new ArrayList<>(storage.getKeys());
      Assertions.assertEquals(List.of(1L), keys);
      Assertions.assertEquals(1, storage.getLastKey());
    } finally {
      if (storage != null) {
        storage.delete();
      }
      if (file.exists()) {
        FileHelper.delete(file, true);
      }
    }
  }

  @Test
  void uncleanReopenRemovesOnlyCorruptedMiddleDataRecord() throws IOException {
    File file = new File("test_file" + File.separator + "middleRecordCorruptionRecovery");
    if (file.exists()) {
      FileHelper.delete(file, true);
    }

    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("storeType", "Partition");
    properties.put("Sync", "false");
    properties.put("ItemCount", "100");
    properties.put("ExpiredEventPoll", "120");
    properties.put("MaxPartitionSize", "" + (1024L * 1024L));

    StorageBuilder<MappedData> storageBuilder = new StorageBuilder<>();
    storageBuilder
        .setFactory(getFactory())
        .setName(file.getPath())
        .setProperties(properties);

    Storage<MappedData> storage = null;
    try {
      storage = storageBuilder.build();

      storage.add(createMessageBuilder(1));
      storage.add(createMessageBuilder(2));
      storage.add(createMessageBuilder(3));

      Assertions.assertEquals(3, storage.size());
      Assertions.assertNotNull(storage.get(1));
      Assertions.assertNotNull(storage.get(2));
      Assertions.assertNotNull(storage.get(3));

      storage.close();
      storage = null;


      File indexFile = new File(file, "partition_0_index");
      File dataFile = new File(file, "partition_0_index_data");

      long keyTwoDataPosition = readIndexRecordPosition(indexFile, 2L);
      Assertions.assertTrue(keyTwoDataPosition > 24, "Key 2 should point after key 1 in the data file");

      corruptDataRecordHeader(dataFile, keyTwoDataPosition);

      markFileOpen(indexFile);
      markFileOpen(dataFile);

      Assertions.assertEquals(0xEFFFFFFFFFFFFFFFL, readFileState(indexFile), "Index file must be marked open before recovery");
      Assertions.assertEquals(0xEFFFFFFFFFFFFFFFL, readFileState(dataFile), "Data file must be marked open before recovery");

      storage = storageBuilder.build();

      Assertions.assertEquals(2, storage.size());

      Assertions.assertTrue(storage.contains(1));
      Assertions.assertFalse(storage.contains(2));
      Assertions.assertTrue(storage.contains(3));

      MappedData first = storage.get(1);
      Assertions.assertNotNull(first);
      Assertions.assertEquals(1, first.getKey());

      Assertions.assertNull(storage.get(2));

      MappedData third = storage.get(3);
      Assertions.assertNotNull(third);
      Assertions.assertEquals(3, third.getKey());

      List<Long> keys = new ArrayList<>(storage.getKeys());
      Assertions.assertEquals(List.of(1L, 3L), keys);
      Assertions.assertEquals(3, storage.getLastKey());
    } finally {
      if (storage != null) {
        storage.delete();
      }
      if (file.exists()) {
        FileHelper.delete(file, true);
      }
    }
  }

  private long readIndexRecordPosition(File indexFile, long key) throws IOException {
    long indexHeaderSize = 32L;
    long indexManagerHeaderSize = 16L;

    try (RandomAccessFile randomAccessFile = new RandomAccessFile(indexFile, "r")) {
      randomAccessFile.seek(indexHeaderSize);
      long partitionStart = randomAccessFile.readLong();
      long partitionEnd = randomAccessFile.readLong();

      Assertions.assertTrue(
          key >= partitionStart && key <= partitionEnd,
          "Key should be inside the partition range, key=" + key
              + ", partitionStart=" + partitionStart
              + ", partitionEnd=" + partitionEnd);

      long slot = key - partitionStart;
      long recordOffset = indexHeaderSize + indexManagerHeaderSize + (slot * IndexRecord.HEADER_SIZE);

      randomAccessFile.seek(recordOffset);
      long position = randomAccessFile.readLong();

      Assertions.assertTrue(
          position > 0,
          "Index record should point to a data record, key=" + key
              + ", slot=" + slot
              + ", recordOffset=" + recordOffset
              + ", position=" + position);

      return position;
    }
  }

  private void corruptDataRecordHeader(File dataFile, long position) throws IOException {
    try (RandomAccessFile randomAccessFile = new RandomAccessFile(dataFile, "rw")) {
      randomAccessFile.seek(position);
      randomAccessFile.writeInt(-1);
    }
  }

  @Test
  void uncleanReopenIgnoresTrailingGarbageNotReferencedByIndex() throws IOException {
    File file = new File("test_file" + File.separator + "trailingGarbageRecovery");
    if (file.exists()) {
      FileHelper.delete(file, true);
    }

    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("storeType", "Partition");
    properties.put("Sync", "false");
    properties.put("ItemCount", "100");
    properties.put("ExpiredEventPoll", "120");
    properties.put("MaxPartitionSize", "" + (1024L * 1024L));

    StorageBuilder<MappedData> storageBuilder = new StorageBuilder<>();
    storageBuilder
        .setFactory(getFactory())
        .setName(file.getPath())
        .setProperties(properties);

    Storage<MappedData> storage = null;
    try {
      storage = storageBuilder.build();

      storage.add(createMessageBuilder(1));
      storage.add(createMessageBuilder(2));

      Assertions.assertEquals(2, storage.size());
      Assertions.assertNotNull(storage.get(1));
      Assertions.assertNotNull(storage.get(2));

      storage.close();
      storage = null;

      File indexFile = new File(file, "partition_0_index");
      File dataFile = new File(file, "partition_0_index_data");

      try (RandomAccessFile randomAccessFile = new RandomAccessFile(dataFile, "rw")) {
        randomAccessFile.seek(randomAccessFile.length());
        randomAccessFile.write(new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
      }

      markFileOpen(indexFile);
      markFileOpen(dataFile);

      Assertions.assertEquals(
          0xEFFFFFFFFFFFFFFFL,
          readFileState(indexFile),
          "Index file must be marked open before recovery");

      Assertions.assertEquals(
          0xEFFFFFFFFFFFFFFFL,
          readFileState(dataFile),
          "Data file must be marked open before recovery");

      storage = storageBuilder.build();

      Assertions.assertEquals(2, storage.size());
      Assertions.assertTrue(storage.contains(1));
      Assertions.assertTrue(storage.contains(2));

      MappedData first = storage.get(1);
      Assertions.assertNotNull(first);
      Assertions.assertEquals(1, first.getKey());

      MappedData second = storage.get(2);
      Assertions.assertNotNull(second);
      Assertions.assertEquals(2, second.getKey());

      List<Long> keys = new ArrayList<>(storage.getKeys());
      Assertions.assertEquals(List.of(1L, 2L), keys);
      Assertions.assertEquals(2, storage.getLastKey());
    } finally {
      if (storage != null) {
        storage.delete();
      }
      if (file.exists()) {
        FileHelper.delete(file, true);
      }
    }
  }

  private void markFileOpen(File file) throws IOException {
    try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
      randomAccessFile.seek(0);
      randomAccessFile.writeLong(0xEFFFFFFFFFFFFFFFL);
    }
  }

  private long readFileState(File file) throws IOException {
    try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
      randomAccessFile.seek(0);
      return randomAccessFile.readLong();
    }
  }

  @Test
  void removedNewestSparseKeyDoesNotRemainLastKeyAfterReopen() throws IOException {
    File file = new File("test_file" + File.separator + "removedNewestSparseKeyAfterReopen");
    if (file.exists()) {
      FileHelper.delete(file, true);
    }

    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("storeType", "Partition");
    properties.put("Sync", "false");
    properties.put("ItemCount", "100");
    properties.put("ExpiredEventPoll", "120");
    properties.put("MaxPartitionSize", "" + (1024L * 1024L));

    StorageBuilder<MappedData> storageBuilder = new StorageBuilder<>();
    storageBuilder
        .setFactory(getFactory())
        .setName(file.getPath())
        .setProperties(properties);

    Storage<MappedData> storage = null;
    try {
      storage = storageBuilder.build();

      storage.add(createMessageBuilder(1_000_000));
      storage.add(createMessageBuilder(1_000_050));
      Assertions.assertTrue(storage.remove(1_000_050));

      storage.close();
      storage = null;

      storage = storageBuilder.build();

      Assertions.assertEquals(1_000_000, storage.getLastKey());
      Assertions.assertTrue(storage.contains(1_000_000));
      Assertions.assertFalse(storage.contains(1_000_050));

      MappedData older = storage.get(1_000_000);
      Assertions.assertNotNull(older);
      Assertions.assertEquals(1_000_000, older.getKey());

      List<Long> keys = new ArrayList<>(storage.getKeys());
      Assertions.assertEquals(List.of(1_000_000L), keys);
    } finally {
      if (storage != null) {
        storage.delete();
      }
      if (file.exists()) {
        FileHelper.deleteAllowingNfsTemporaryFiles(file, true);
      }
    }
  }

  @Test
  void sparseKeysRetainNaturalOrderAndLastKeyAfterReopen() throws IOException {
    File file = new File("test_file" + File.separator + "sparseKeysLastKeyAfterReopen");
    if (file.exists()) {
      FileHelper.delete(file, true);
    }

    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("storeType", "Partition");
    properties.put("Sync", "false");
    properties.put("ItemCount", "100");
    properties.put("ExpiredEventPoll", "120");
    properties.put("MaxPartitionSize", "" + (1024L * 1024L));

    StorageBuilder<MappedData> storageBuilder = new StorageBuilder<>();
    storageBuilder
        .setFactory(getFactory())
        .setName(file.getPath())
        .setProperties(properties);

    Storage<MappedData> storage = null;
    try {
      storage = storageBuilder.build();

      storage.add(createMessageBuilder(1_000_050));
      storage.add(createMessageBuilder(1_000_000));

      Assertions.assertEquals(1_000_050, storage.getLastKey());
      storage.close();
      storage = null;

      storage = storageBuilder.build();

      Assertions.assertEquals(1_000_050, storage.getLastKey());
      Assertions.assertTrue(storage.contains(1_000_000));
      Assertions.assertTrue(storage.contains(1_000_050));

      MappedData older = storage.get(1_000_000);
      Assertions.assertNotNull(older);
      Assertions.assertEquals(1_000_000, older.getKey());

      MappedData newer = storage.get(1_000_050);
      Assertions.assertNotNull(newer);
      Assertions.assertEquals(1_000_050, newer.getKey());

      List<Long> keys = new ArrayList<>(storage.getKeys());
      Assertions.assertEquals(List.of(1_000_000L, 1_000_050L), keys);
    } finally {
      if (storage != null) {
        storage.delete();
      }
      if (file.exists()) {
        FileHelper.deleteAllowingNfsTemporaryFiles(file, true);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void s3ArchiveAndRestorePartition(boolean compress) throws IOException, InterruptedException {
    String accessKeyId = System.getProperty("accessKeyId");
    String secretAccessKey = System.getProperty("secretAccessKey");
    String region = System.getProperty("regionName");
    String bucketName = System.getProperty("bucketName");

    if(accessKeyId != null && secretAccessKey != null && region != null && bucketName != null) {
      S3Client amazonS3 = createS3Client(accessKeyId, secretAccessKey, region);

      Assertions.assertTrue(isBucketEmpty(amazonS3, bucketName), "S3 bucket should be empty before the test starts");
      Map<String, String> properties = buildProperties(false);
      properties.put("deferredName", "S3");
      properties.put("archiveIdleTime", ""+TimeUnit.SECONDS.toMillis(4));
      properties.put("S3AccessKeyId", accessKeyId);
      properties.put("S3SecretAccessKey",secretAccessKey);
      properties.put("S3RegionName", region);
      properties.put("S3BucketName", bucketName);
      properties.put("S3CompressEnabled", ""+compress);
      Storage<MappedData> storage = build(properties,  BasePartitionStoreTest.computeNameFromTestName(testName));
      for (int x = 0; x < 1100; x++) {
        MappedData message = createMessageBuilder(x);
        storage.add(message);
      }

      // We should have exceeded the partition limits and have 10 partitions, lets wait the time out period
      TimeUnit.SECONDS.sleep(5);
      ((TierMigrationMonitor)storage).scanForArchiveMigration();
      Assertions.assertEquals(10, getBucketEntityCount(amazonS3, bucketName), "S3 bucket should have ten entries");

      // They should now be archived
      for (int x = 0; x < 1100; x++) {
        MappedData data = storage.get(x);
        Assertions.assertNotNull(data, "Expected data for key "+x);
        Assertions.assertEquals(data.key, x);

      }
      Assertions.assertTrue(isBucketEmpty(amazonS3, bucketName), "S3 bucket should be empty when the test finishes");
      storage.delete();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void s3ArchiveAndDeleteStore(boolean compress) throws IOException, InterruptedException {
    String accessKeyId = System.getProperty("accessKeyId");
    String secretAccessKey = System.getProperty("secretAccessKey");
    String region = System.getProperty("regionName");
    String bucketName = System.getProperty("bucketName");
    if(accessKeyId != null && secretAccessKey != null && region != null && bucketName != null) {
      S3Client amazonS3 = createS3Client(accessKeyId, secretAccessKey, region);

      Assertions.assertTrue(isBucketEmpty(amazonS3, bucketName), "S3 bucket should be empty before the test starts");
      Map<String, String> properties = buildProperties(false);
      properties.put("deferredName", "S3");
      properties.put("archiveIdleTime", ""+TimeUnit.SECONDS.toMillis(30));
      properties.put("S3AccessKeyId", accessKeyId);
      properties.put("S3SecretAccessKey",secretAccessKey);
      properties.put("S3RegionName", region);
      properties.put("S3BucketName", bucketName);
      properties.put("S3CompressEnabled", ""+compress);
      Storage<MappedData> storage = build(properties,  BasePartitionStoreTest.computeNameFromTestName(testName));
      for (int x = 0; x < 1100; x++) {
        MappedData message = createMessageBuilder(x);
        storage.add(message);
      }

      // We should have exceeded the partition limits and have 10 partitions, lets wait the time out period
      TimeUnit.SECONDS.sleep(40);
      ((TierMigrationMonitor)storage).scanForArchiveMigration();
      Assertions.assertEquals(10, getBucketEntityCount(amazonS3, bucketName), "S3 bucket should have ten entries");
      storage.delete();
      Assertions.assertTrue(isBucketEmpty(amazonS3, bucketName), "S3 bucket should be empty when the test finishes");
    }
  }

  private S3Client createS3Client(String accessKeyId, String secretAccessKey, String region) {
    AwsBasicCredentials credentials = AwsBasicCredentials.create(accessKeyId, secretAccessKey);
    return S3Client.builder()
        .region(Region.of(region))
        .credentialsProvider(StaticCredentialsProvider.create(credentials))
        .build();
  }

  boolean isBucketEmpty(S3Client s3Client, String bucketName) {
    return getBucketEntityCount(s3Client, bucketName) == 0;
  }

  int getBucketEntityCount(S3Client s3Client, String bucketName) {
    ListObjectsV2Request request = ListObjectsV2Request.builder()
        .bucket(bucketName)
        .build();
    ListObjectsV2Response response = s3Client.listObjectsV2(request);
    return response.keyCount();
  }
}
