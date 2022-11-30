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

package io.mapsmessaging.storage.impl.file.partition.archive.compress;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class CompressionHelper {

  public long compress(String fileName) throws IOException {
    File file = new File(fileName);
    File zipFile = new File(fileName+"_zip");
    long length = file.length();

    try(FileOutputStream fileOutputStream = new FileOutputStream(zipFile)){
      try(GZIPOutputStream gzipOutputStream = new GZIPOutputStream(fileOutputStream)) {
        try (FileInputStream fileInputStream = new FileInputStream(file)) {
          byte[] tmp = new byte[1024];
          int count = 0;
          while (count < length) {
            int len = fileInputStream.read(tmp, 0, tmp.length);
            if (len > 0) {
              gzipOutputStream.write(tmp, 0, len);
              count += len;
            }
          }
          gzipOutputStream.flush();
        }
      }
    }
    file.delete();
    return length;
  }

  public void decompress(String fileName) throws IOException {
    File file = new File(fileName);
    File zipFile = new File(fileName+"_zip");

    try(FileInputStream fileInputStream = new FileInputStream(zipFile)){
      try(GZIPInputStream gzipInputStream = new GZIPInputStream(fileInputStream)) {
        try (FileOutputStream fileOutputStream = new FileOutputStream(file)) {
          byte[] tmp = new byte[1024];
          int len = 1;
          while (len > 0) {
            len = gzipInputStream.read(tmp, 0, tmp.length);
            if (len > 0) {
              fileOutputStream.write(tmp, 0, len);
            }
          }
          fileOutputStream.flush();
        }
      }
    }
    zipFile.delete();
  }
}
