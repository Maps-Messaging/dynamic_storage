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

package io.mapsmessaging.storage.impl;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

@State(Scope.Benchmark)
public class PackUnpackBenchmark {

  AtomicLong counter = new AtomicLong(0);

  protected byte[] toByteArray(long val, int size) {
    byte[] tmp = new byte[size];
    for (int x = 0; x < size; x++) {
      tmp[(size - x) - 1] = (byte) (0xff & val);
      val = val >> 8;
    }
    return tmp;
  }


  protected byte[] toByteArrayFromLong(long val) {
    byte[] tmp = new byte[8];
    tmp[7] = (byte) ((val >>> 56) & 0xFF);
    tmp[6] = (byte) ((val >>> 48) & 0xFF);
    tmp[5] = (byte) ((val >>> 40) & 0xFF);
    tmp[4] = (byte) ((val >>> 32) & 0xFF);
    tmp[3] = (byte) ((val >>> 24) & 0xFF);
    tmp[2] = (byte) ((val >>> 16) & 0xFF);
    tmp[1] = (byte) ((val >>> 8) & 0xFF);
    tmp[0] = (byte) ((val) & 0xFF);
    return tmp;
  }

  @Benchmark
  @BenchmarkMode({Mode.Throughput})
  @Fork(value = 1, warmups = 2)
  @Threads(12)
  public void existingAlgorithm(Blackhole blackhole) {
    blackhole.consume(toByteArray(counter.incrementAndGet(), 8));
  }

  @Benchmark
  @BenchmarkMode({Mode.Throughput})
  @Fork(value = 1, warmups = 2)
  @Threads(12)
  public void unwoundAlgorithm(Blackhole blackhole) {
    blackhole.consume(toByteArrayFromLong(counter.incrementAndGet()));
  }

  @Benchmark
  @BenchmarkMode({Mode.Throughput})
  @Fork(value = 1, warmups = 2)
  @Threads(12)
  public void unsafeByteBuffer(Blackhole blackhole) {
    ByteBuffer bb = ByteBuffer.allocate(16);
    bb.putLong(counter.incrementAndGet());
    blackhole.consume(bb);
  }

}
