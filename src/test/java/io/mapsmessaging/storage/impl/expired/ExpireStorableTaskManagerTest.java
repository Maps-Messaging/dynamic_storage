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

package io.mapsmessaging.storage.impl.expired;

import io.mapsmessaging.storage.ExpiredMonitor;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.impl.file.TaskQueue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

class ExpireStorableTaskManagerTest {

  @Test
  void continuesPollingWhenTheFirstScanFindsNothingExpired() throws Exception {
    CountDownLatch scans = new CountDownLatch(2);
    TaskQueue taskQueue = new TaskQueue();
    ExpiredMonitor monitor = new ExpiredMonitor() {
      @Override
      public void scanForExpired() {
        scans.countDown();
      }

      @Override
      public boolean hasExpiringEntries() {
        return true;
      }
    };

    try (ExpireStorableTaskManager<TestStorable> manager =
             new ExpireStorableTaskManager<>(monitor, taskQueue, 1)) {
      manager.added(new TestStorable(1, System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10)));

      Assertions.assertTrue(scans.await(3, TimeUnit.SECONDS),
          "Expected a second expiry scan after the first scan found no expired records");
    }
  }

  @Test
  void disabledPollingDoesNotScheduleExpiryScans() throws Exception {
    CountDownLatch scans = new CountDownLatch(1);
    TaskQueue taskQueue = new TaskQueue();

    try (ExpireStorableTaskManager<TestStorable> manager =
             new ExpireStorableTaskManager<>(scans::countDown, taskQueue, -1)) {
      manager.added(new TestStorable(1, System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10)));

      Assertions.assertFalse(scans.await(1200, TimeUnit.MILLISECONDS));
    }
  }

  @Test
  void stopsPollingWhenNoExpiringEntriesRemain() throws Exception {
    CountDownLatch scans = new CountDownLatch(2);
    AtomicInteger scanCount = new AtomicInteger();
    TaskQueue taskQueue = new TaskQueue();

    ExpiredMonitor monitor = new ExpiredMonitor() {
      @Override
      public void scanForExpired() {
        scanCount.incrementAndGet();
        scans.countDown();
      }

      @Override
      public boolean hasExpiringEntries() {
        return scanCount.get() < 2;
      }
    };

    try (ExpireStorableTaskManager<TestStorable> manager =
             new ExpireStorableTaskManager<>(monitor, taskQueue, 1)) {
      manager.added(new TestStorable(1, System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10)));

      Assertions.assertTrue(scans.await(3, TimeUnit.SECONDS));
      TimeUnit.MILLISECONDS.sleep(1200);
      Assertions.assertEquals(2, scanCount.get(),
          "Expiry polling should stop when the store has no expiring entries");
    }
  }

  private record TestStorable(long key, long expiry) implements Storable {

    @Override
    public long getKey() {
      return key;
    }

    @Override
    public long getExpiry() {
      return expiry;
    }
  }
}
