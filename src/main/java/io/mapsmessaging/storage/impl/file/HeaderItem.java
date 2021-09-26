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

package io.mapsmessaging.storage.impl.file;

import java.nio.ByteBuffer;
import java.util.Arrays;
import lombok.Getter;
import lombok.Setter;

public class HeaderItem {

  public static final int HEADER_SIZE = 16;
  private static final byte[] RESET_HEADER;
  static{
    RESET_HEADER = new byte[HEADER_SIZE];
    Arrays.fill(RESET_HEADER, (byte)0);
  }

  @Getter private final long locationId; // If 0 then located within the index file, else is the unique ID of the log file
  @Getter private final long position;   // Position within the log file or the index file
  @Getter private final long expiry;     // Expiry of this entry
  @Getter private final long length;     // Expiry of this entry

  @Getter @Setter private long key;

  HeaderItem(){
    locationId = 0;
    position = 0;
    expiry =0;
    length = 0;
  }

  public HeaderItem(long locationId, long position, long expiry, long length){
    this.locationId = locationId;
    this.position = position;
    this.expiry = expiry;
    this.length = length;
  }

  public HeaderItem(ByteBuffer buffer){
    long tmp1 = buffer.getLong();
    long tmp2 = buffer.getLong();

    locationId = tmp1 >> 32;
    position = tmp1 & 0xFFFFFFFFL;

    expiry = tmp2 >> 32;
    length = tmp2 & 0xFFFFFFFFL;
  }

  public void update(ByteBuffer buffer){
    long tmp1 = locationId & 0xEFFFFFFFL << 32;
    tmp1 = tmp1 | (position & 0xFFFFFFFFL);

    long tmp2 = expiry & 0xEFFFFFFFL << 32;
    tmp2 = tmp2 | (length & 0xFFFFFFFFL);

    buffer.putLong(tmp1);
    buffer.putLong(tmp2);
  }

  public static void clear(ByteBuffer buffer){
    buffer.put(RESET_HEADER);
  }
}
