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

package io.mapsmessaging.storage.impl.file.partition.deferred;

import lombok.Getter;
import lombok.Setter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.time.LocalDateTime;
import java.util.Objects;

public abstract class DeferredRecord {

  @Getter
  @Setter
  protected String digestName;

  @Getter
  @Setter
  protected String deferredHash;

  @Getter
  @Setter
  private long length;

  @Getter
  @Setter
  protected LocalDateTime archivedDate;

  protected DeferredRecord(){}

  protected DeferredRecord(String digestName, String arcdeferredHash, long length){
    this.digestName = digestName;
    this.deferredHash = Objects.requireNonNullElse(arcdeferredHash, "");
    this.length = length;
    this.archivedDate = LocalDateTime.now();
  }

  public abstract void read(String filename) throws IOException;

  protected void readIn(BufferedReader reader) throws IOException {
    digestName = reader.readLine();
    deferredHash = reader.readLine();
    setLength(Long.parseLong(reader.readLine()));
    archivedDate = LocalDateTime.parse(reader.readLine());
  }

  protected void writeOut(OutputStreamWriter writer) throws IOException{
    writer.write(digestName+"\n");
    writer.write(deferredHash + "\n");
    writer.write(""+getLength()+"\n");
    writer.write(archivedDate.toString() + "\n");
  }
}
