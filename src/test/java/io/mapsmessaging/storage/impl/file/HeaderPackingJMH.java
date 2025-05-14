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

package io.mapsmessaging.storage.impl.file;


import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.ByteBuffer;

@State(Scope.Benchmark)
public class HeaderPackingJMH {


  @Benchmark
  @BenchmarkMode({Mode.All})
  @Fork(value = 1, warmups = 2)
  @Threads(12)
  public void performIntTasks(Blackhole blackhole)  {
    ByteBuffer test = ByteBuffer.allocate(128);
    writeInt(312321312L, 3756878L, 546347645L, 3167L, test);
    test.flip();
    readInt(blackhole, test);
  }

  @Benchmark
  @BenchmarkMode({Mode.All})
  @Fork(value = 1, warmups = 2)
  @Threads(12)
  public void performPackedIntToLongTasks(Blackhole blackhole)  {
    ByteBuffer test = ByteBuffer.allocate(128);
    writeIntToLong(312321312L, 3756878L, 546347645L, 3167L, test);
    test.flip();
    readIntToLong(blackhole, test);
  }

  @Benchmark
  @BenchmarkMode({Mode.All})
  @Fork(value = 1, warmups = 2)
  @Threads(12)
  public void performLongTasks(Blackhole blackhole) {
    ByteBuffer test = ByteBuffer.allocate(128);
    writeLong(312321312L, 3756878L, 546347645L, 3167L, test);
    test.flip();
    readLong(blackhole, test);
  }


  public void readIntToLong(Blackhole blackhole, ByteBuffer buffer) {
    long tmp1 = buffer.getLong();
    long tmp2 = buffer.getLong();

    blackhole.consume(tmp1 >> 32);
    blackhole.consume(tmp1 & 0xFFFFFFFFL);
    blackhole.consume(tmp2 >> 32);
    blackhole.consume(tmp2 & 0xFFFFFFFFL);
  }

  public void writeIntToLong(long locationId, long position, long expiry, long length, ByteBuffer buffer) {
    long tmp1 = locationId & 0xEFFFFFFFL << 32;
    tmp1 = tmp1 | (position & 0xFFFFFFFFL);

    long tmp2 = expiry & 0xEFFFFFFFL << 32;
    tmp2 = tmp2 | (length & 0xFFFFFFFFL);

    buffer.putLong(tmp1);
    buffer.putLong(tmp2);
  }

  public void readInt(Blackhole blackhole, ByteBuffer buffer) {
    blackhole.consume(buffer.getInt());
    blackhole.consume(buffer.getInt());
    blackhole.consume(buffer.getInt());
    blackhole.consume(buffer.getInt());
  }

  public void writeInt(long locationId, long position, long expiry, long length, ByteBuffer buffer) {
    buffer.putInt((int) (locationId & 0xffffffffL));
    buffer.putInt((int) (position & 0xffffffffL));
    buffer.putInt((int) (expiry & 0xffffffffL));
    buffer.putInt((int) (length & 0xffffffffL));
  }

  public void readLong(Blackhole blackhole, ByteBuffer buffer) {
    blackhole.consume(buffer.getLong());
    blackhole.consume(buffer.getLong());
    blackhole.consume(buffer.getLong());
    blackhole.consume(buffer.getLong());
  }

  public void writeLong(long locationId, long position, long expiry, long length, ByteBuffer buffer) {
    buffer.putLong(locationId);
    buffer.putLong(position);
    buffer.putLong(expiry);
    buffer.putLong(length);
  }
}
