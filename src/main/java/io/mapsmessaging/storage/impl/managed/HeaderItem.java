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

package io.mapsmessaging.storage.impl.managed;

import java.nio.ByteBuffer;
import java.util.Arrays;
import lombok.Getter;
import lombok.Setter;

public class HeaderItem {

  public static final int HEADER_SIZE = 24;
  private static final byte[] RESET_HEADER;
  static{
    RESET_HEADER = new byte[HEADER_SIZE];
    Arrays.fill(RESET_HEADER, (byte)0);
  }

  @Getter private final long locationId; // If 0 then located within the index file, else is the unique ID of the log file
  @Getter private final long position;   // Position within the log file or the index file
  @Getter private final long expiry;     // Expiry of this entry
  @Getter @Setter private long key;

  public HeaderItem(long locationId, long position, long expiry){
    this.locationId = locationId;
    this.position = position;
    this.expiry = expiry;
  }

  public HeaderItem(ByteBuffer buffer){
    locationId = buffer.getLong();
    position = buffer.getLong();
    expiry = buffer.getLong();
  }

  public void update(ByteBuffer buffer){
    buffer.putLong(locationId);
    buffer.putLong(position);
    buffer.putLong(expiry);
  }

  public static void clear(ByteBuffer buffer){
    buffer.put(RESET_HEADER);
  }
}
