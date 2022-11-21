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

package io.mapsmessaging.storage.impl.file.s3;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.time.LocalDateTime;
import lombok.Getter;
import lombok.Setter;

public class S3Record {

  private static final String HEADER = "s3 bucket place holder";
  @Getter
  @Setter
  private String bucketName;

  @Getter
  @Setter
  private String entryName;

  @Getter
  @Setter
  private String md5;

  @Getter
  @Setter
  private long length;

  @Getter
  @Setter
  private LocalDateTime archivedDate;

  public S3Record() {
  }

  public S3Record(String bucketName, String entryName, String contentMd5, long length) {
    this.archivedDate = LocalDateTime.now();
    this.bucketName = bucketName;
    this.entryName = entryName;
    this.md5 = contentMd5;
    this.length = length;
  }

  public void write(String fileName) throws IOException {
    FileOutputStream fileOutputStream = new FileOutputStream(fileName, false);
    try (OutputStreamWriter writer = new OutputStreamWriter(fileOutputStream)) {
      writer.write(HEADER +"\n");
      writer.write(bucketName + "\n");
      writer.write(entryName + "\n");
      writer.write(md5 + "\n");
      writer.write(""+length+"\n");
      writer.write(archivedDate.toString() + "\n");
    }
  }

  public void read(String fileName) throws IOException{
    FileReader fileReader = new FileReader(fileName);
    try (BufferedReader reader = new BufferedReader(fileReader)) {
      if(!reader.readLine().equals(HEADER)){
        throw new IOException("Invalid place holder");
      }
      bucketName = reader.readLine();
      entryName = reader.readLine();
      md5 = reader.readLine();
      length = Long.parseLong(reader.readLine());
      archivedDate = LocalDateTime.parse(reader.readLine());
    }
  }
}
