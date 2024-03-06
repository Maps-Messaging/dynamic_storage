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

import io.mapsmessaging.logging.Logger;
import io.mapsmessaging.logging.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static io.mapsmessaging.storage.logging.StorageLogMessages.*;

public class FileHelper {

  private static final Logger logger = LoggerFactory.getLogger(FileHelper.class);

  public static boolean delete(String fileName) throws IOException {
    return delete(new File(fileName));
  }

  public static boolean delete(String fileName, boolean andChildren, boolean suppressException) throws IOException {
    if(suppressException) {
      try {
        return delete(new File(fileName), andChildren);
      } catch (IOException e) {
        logger.log(FILE_HELPER_EXCEPTION_RAISED, e, fileName);
      }
    }
    return delete(new File(fileName), andChildren);
  }


  public static boolean delete(File file) throws IOException {
    return delete(file, false);
  }

  public static boolean delete(File file, boolean andChildren) throws IOException {
    if(file.exists()){
      if(file.isDirectory() && andChildren){
        File[] children = file.listFiles();
        if(children != null) {
          for (File child : children) {
            delete(child, andChildren);
          }
        }
        Files.deleteIfExists(file.toPath());
        logger.log(FILE_HELPER_DELETED_FILE, file.toString());

      }
      else{
        return deleteExistingFile(file);
      }
    }
    else{
      logger.log(FILE_HELPER_FILE_DOES_NOT_EXIST, file.toString());
      throw new IOException("File does not exist");
    }
    return true;
  }

  private static boolean deleteExistingFile(File file) throws IOException {
    Files.deleteIfExists(file.toPath());
    logger.log(FILE_HELPER_DELETED_FILE, file.toString());
    return true;
  }

  private FileHelper(){
    // Hide the constructor
  }
}
