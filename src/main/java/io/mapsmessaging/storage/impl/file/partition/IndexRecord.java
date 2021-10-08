/*
 *
 * Copyright [2020 - 2021]   [Matthew Buckton]
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 *
 */

package io.mapsmessaging.storage.impl.file.partition;

import lombok.Getter;
import lombok.Setter;

import java.nio.ByteBuffer;

public class IndexRecord {
  private static final long INTEGER_MASK = 0x7FFF_FFFF;

  public static final int HEADER_SIZE = 24;

  @Getter private final long expiry;     // Expiry of this entry in milliseconds
  @Getter private final long position;   // Position within the log file or the index file

  @Getter private final int locationId; // If 0 then located within the index file, else is the unique ID of the data file
  @Getter private final int length;     // The number of bytes that the record consumes

  @Getter @Setter private long key;      // The key, this is calculated and is NOT stored in the header!

  IndexRecord(){
    position = 0;
    expiry =0;

    locationId = 0;
    length = 0;
  }

  public IndexRecord(int locationId, long position, long expiry, int length){
    this.position = position;
    this.expiry = expiry;

    this.locationId = locationId;
    this.length = length;
  }

  public IndexRecord(ByteBuffer buffer){
    position = buffer.getLong();
    expiry = buffer.getLong();
    long tmp2 = buffer.getLong();

    locationId = (int)(tmp2 >> 32);
    length = (int) (tmp2 & INTEGER_MASK);
  }

  public void update(ByteBuffer buffer){
    long tmp2 = ((locationId & INTEGER_MASK) << 32) | (length & INTEGER_MASK);
    buffer.putLong(position);
    buffer.putLong(expiry);
    buffer.putLong(tmp2);
  }
}
