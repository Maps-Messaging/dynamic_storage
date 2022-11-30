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

import io.mapsmessaging.storage.impl.file.partition.archive.ArchiveRecord;
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.time.LocalDateTime;
import lombok.Getter;
import lombok.Setter;

public class CompressionRecord extends ArchiveRecord {

  private static final String HEADER = "# Zip file place holder";

  @Getter
  @Setter
  private LocalDateTime archivedDate;

  public CompressionRecord() {
  }

  public CompressionRecord(long length) {
    super(length);
    this.archivedDate = LocalDateTime.now();
  }

  public void write(String fileName) throws IOException {
    FileOutputStream fileOutputStream = new FileOutputStream(fileName, false);
    try (OutputStreamWriter writer = new OutputStreamWriter(fileOutputStream)) {
      writer.write(HEADER +"\n");
      writer.write(""+getLength()+"\n");
      writer.write(archivedDate.toString() + "\n");
    }
  }

  public void read(String fileName) throws IOException{
    FileReader fileReader = new FileReader(fileName);
    try (BufferedReader reader = new BufferedReader(fileReader)) {
      if(!reader.readLine().equals(HEADER)){
        throw new IOException("Invalid place holder");
      }
      setLength(Long.parseLong(reader.readLine()));
      archivedDate = LocalDateTime.parse(reader.readLine());
    }
  }
}
