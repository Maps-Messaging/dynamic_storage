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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class FileHelper {

  public static boolean delete(String fileName) throws IOException {
    return delete(new File(fileName));
  }

  public static boolean delete(String fileName, boolean andChildren, boolean suppressException) throws IOException {
    if(suppressException) {
      try {
        return delete(new File(fileName), andChildren);
      } catch (IOException e) {
        e.printStackTrace();
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
      }
    }
    else{
      throw new IOException("File does not exist");
    }
    return true;
  }

}
