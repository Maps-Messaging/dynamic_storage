/*
 *    Copyright [ 2020 - 2024 ] [Matthew Buckton]
 *    Copyright [ 2024 - 2025 ] [Maps Messaging B.V.]
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
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

package io.mapsmessaging.storage.impl.file.partition.archive.migration;

import io.mapsmessaging.storage.impl.file.partition.archive.ArchiveRecord;
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class MigrationRecord extends ArchiveRecord {

  private static final String HEADER = "# Migration file place holder";

  public MigrationRecord() {
  }

  public MigrationRecord(long length, String hash, String digestName) {
    super(digestName, hash, length);
  }

  public void write(String fileName) throws IOException {
    try (FileOutputStream fileOutputStream = new FileOutputStream(fileName, false)) {
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