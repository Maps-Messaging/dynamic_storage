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

package io.mapsmessaging.storage.impl.file;

import io.mapsmessaging.logging.Logger;
import io.mapsmessaging.logging.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static io.mapsmessaging.storage.logging.StorageLogMessages.*;

public class FileHelper {

  private static final Logger logger = LoggerFactory.getLogger(FileHelper.class);

  private static final int DELETE_ATTEMPTS = 10;
  private static final long DELETE_RETRY_NANOS = 1_000_000L;

  private static final int NFS_DELETE_ATTEMPTS = 20;
  private static final long NFS_DELETE_DELAY_MS = 250L;

  private static final ScheduledExecutorService DELETE_WATCHER =
      Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = new Thread(runnable, "dynamic-storage-delete-watcher");
        thread.setDaemon(true);
        return thread;
      });

  public static boolean delete(String fileName) throws IOException {
    return delete(new File(fileName));
  }

  public static boolean delete(String fileName, boolean andChildren, boolean suppressException) throws IOException {
    if (suppressException) {
      try {
        return delete(new File(fileName), andChildren);
      } catch (IOException e) {
        logger.log(FILE_HELPER_EXCEPTION_RAISED, e, fileName);
        return false;
      }
    }
    return delete(new File(fileName), andChildren);
  }

  public static boolean delete(File file) throws IOException {
    return delete(file, false);
  }

  public static boolean delete(File file, boolean andChildren) throws IOException {
    if (!file.exists()) {
      logger.log(FILE_HELPER_FILE_DOES_NOT_EXIST, file.toString());
      throw new IOException("File does not exist " + file);
    }

    if (file.isDirectory() && andChildren) {
      File[] children = file.listFiles();
      if (children != null) {
        for (File child : children) {
          delete(child, true);
        }
      }
      Files.deleteIfExists(file.toPath());
      logger.log(FILE_HELPER_DELETED_FILE, file.toString());
      return true;
    }

    return deleteExistingFile(file);
  }

  public static boolean deleteAllowingNfsTemporaryFiles(File file, boolean andChildren) throws IOException {
    if (!file.exists()) {
      logger.log(FILE_HELPER_FILE_DOES_NOT_EXIST, file.toString());
      throw new IOException("File does not exist " + file);
    }

    if (file.isDirectory() && andChildren) {
      deleteDirectoryAllowingNfsTemporaryFiles(file);
      return true;
    }

    if (isNfsTemporaryFile(file)) {
      return true;
    }

    return deleteExistingFile(file);
  }

  public static boolean deleteAllowingNfsTemporaryFiles(String fileName, boolean andChildren) throws IOException {
    return deleteAllowingNfsTemporaryFiles(new File(fileName), andChildren);
  }

  private static void deleteDirectoryAllowingNfsTemporaryFiles(File directory) throws IOException {
    deleteChildrenAllowingNfsTemporaryFiles(directory);

    if (deleteDirectoryIfEmpty(directory)) {
      return;
    }

    if (containsOnlyNfsTemporaryFiles(directory)) {
      scheduleDirectoryDelete(directory, NFS_DELETE_ATTEMPTS);
      return;
    }

    String[] children = directory.list();
    if (children != null && children.length > 0) {
      throw new IOException(
          "Unable to delete directory " + directory + ", remaining files: " + String.join(", ", children));
    }
  }

  private static void deleteChildrenAllowingNfsTemporaryFiles(File directory) throws IOException {
    File[] children = directory.listFiles();
    if (children == null) {
      return;
    }

    IOException raised = null;
    for (File child : children) {
      try {
        if (isNfsTemporaryFile(child)) {
          continue;
        }

        if (child.isDirectory()) {
          deleteDirectoryAllowingNfsTemporaryFiles(child);
        } else {
          deleteExistingFile(child);
        }
      } catch (IOException e) {
        raised = e;
      }
    }

    if (raised != null) {
      throw raised;
    }
  }

  private static boolean deleteDirectoryIfEmpty(File directory) throws IOException {
    String[] children = directory.list();
    if (children == null || children.length == 0) {
      Files.deleteIfExists(directory.toPath());
      logger.log(FILE_HELPER_DELETED_FILE, directory.toString());
      return true;
    }
    return false;
  }

  private static boolean containsOnlyNfsTemporaryFiles(File directory) {
    String[] children = directory.list();
    if (children == null || children.length == 0) {
      return false;
    }

    for (String child : children) {
      if (!isNfsTemporaryFile(child)) {
        return false;
      }
    }
    return true;
  }

  private static boolean isNfsTemporaryFile(File file) {
    return isNfsTemporaryFile(file.getName());
  }

  private static boolean isNfsTemporaryFile(String name) {
    return name.startsWith(".nfs");
  }

  private static boolean deleteExistingFile(File file) throws IOException {
    int count = 0;
    while (count < DELETE_ATTEMPTS) {
      if (!file.exists()) {
        return true;
      }

      try {
        Files.deleteIfExists(file.toPath());
        logger.log(FILE_HELPER_DELETED_FILE, file.toString());
        return true;
      } catch (IOException e) {
        count++;
        LockSupport.parkNanos(DELETE_RETRY_NANOS);
        if (count == DELETE_ATTEMPTS) {
          throw e;
        }
      }
    }

    return true;
  }

  private static void scheduleDirectoryDelete(File directory, int attemptsRemaining) {
    DELETE_WATCHER.schedule(
        () -> deleteDirectoryWhenNfsFilesReleased(directory, attemptsRemaining),
        NFS_DELETE_DELAY_MS,
        TimeUnit.MILLISECONDS);
  }

  private static void deleteDirectoryWhenNfsFilesReleased(File directory, int attemptsRemaining) {
    try {
      if (!directory.exists()) {
        return;
      }

      deleteChildrenAllowingNfsTemporaryFiles(directory);

      if (deleteDirectoryIfEmpty(directory)) {
        return;
      }

      if (containsOnlyNfsTemporaryFiles(directory) && attemptsRemaining > 0) {
        scheduleDirectoryDelete(directory, attemptsRemaining - 1);
      }
    } catch (IOException e) {
      if (directory.exists() && containsOnlyNfsTemporaryFiles(directory) && attemptsRemaining > 0) {
        scheduleDirectoryDelete(directory, attemptsRemaining - 1);
      }
    }
  }

  private FileHelper() {
    // Hide the constructor.
  }
}