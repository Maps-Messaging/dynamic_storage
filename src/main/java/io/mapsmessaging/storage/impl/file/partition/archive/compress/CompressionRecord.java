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

package io.mapsmessaging.storage.impl.file.partition.archive.compress;

import io.mapsmessaging.storage.impl.file.partition.archive.ArchiveRecord;

import java.io.*;

public class CompressionRecord extends ArchiveRecord {

  private static final String HEADER = "# Zip file place holder";

  public CompressionRecord() {
  }

  public CompressionRecord(long length, String hash, String digestName) {
    super(digestName, hash, length);
  }

  public void write(String fileName) throws IOException {
    try(FileOutputStream fileOutputStream = new FileOutputStream(fileName, false)) {
      try (OutputStreamWriter writer = new OutputStreamWriter(fileOutputStream)) {
        writer.write(HEADER + "\n");
        writeOut(writer);
      }
    }
  }

  public void read(String fileName) throws IOException{
    try(FileReader fileReader = new FileReader(fileName)) {
      try (BufferedReader reader = new BufferedReader(fileReader)) {
        if (!reader.readLine().equals(HEADER)) {
          throw new IOException("Invalid place holder");
        }
        readIn(reader);
      }
    }
  }
}
