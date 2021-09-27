package io.mapsmessaging.storage.impl.file;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.infra.Blackhole;

@State(Scope.Benchmark)
public class HeaderPackingJMH {


  @Benchmark
  @BenchmarkMode({Mode.All})
  @Fork(value = 1, warmups = 2)
  @Threads(12)
  public void performIntTasks(Blackhole blackhole) throws IOException, ExecutionException, InterruptedException {
    ByteBuffer test = ByteBuffer.allocate(128);
    writeInt(312321312L, 3756878L, 546347645L, 3167L, test);
    test.flip();
    readInt(blackhole, test);
  }

  @Benchmark
  @BenchmarkMode({Mode.All})
  @Fork(value = 1, warmups = 2)
  @Threads(12)
  public void performPackedIntToLongTasks(Blackhole blackhole) throws IOException, ExecutionException, InterruptedException {
    ByteBuffer test = ByteBuffer.allocate(128);
    writeIntToLong(312321312L, 3756878L, 546347645L, 3167L, test);
    test.flip();
    readIntToLong(blackhole, test);
  }

  @Benchmark
  @BenchmarkMode({Mode.All})
  @Fork(value = 1, warmups = 2)
  @Threads(12)
  public void performLongTasks(Blackhole blackhole) throws IOException, ExecutionException, InterruptedException {
    ByteBuffer test = ByteBuffer.allocate(128);
    writeLong(312321312L, 3756878L, 546347645L, 3167L, test);
    test.flip();
    readLong(blackhole, test);
  }


  public void readIntToLong(Blackhole blackhole, ByteBuffer buffer){
    long tmp1 = buffer.getLong();
    long tmp2 = buffer.getLong();

    blackhole.consume(tmp1 >> 32);
    blackhole.consume (tmp1 & 0xFFFFFFFFL);
    blackhole.consume (tmp2 >> 32);
    blackhole.consume (tmp2 & 0xFFFFFFFFL);
  }

  public void writeIntToLong(long locationId, long position, long expiry, long length, ByteBuffer buffer){
    long tmp1 = locationId & 0xEFFFFFFFL << 32;
    tmp1 = tmp1 | (position & 0xFFFFFFFFL);

    long tmp2 = expiry & 0xEFFFFFFFL << 32;
    tmp2 = tmp2 | (length & 0xFFFFFFFFL);

    buffer.putLong(tmp1);
    buffer.putLong(tmp2);
  }

  public void readInt(Blackhole blackhole, ByteBuffer buffer){
    blackhole.consume( buffer.getInt());
    blackhole.consume( buffer.getInt());
    blackhole.consume( buffer.getInt());
    blackhole.consume( buffer.getInt());
  }

  public void writeInt(long locationId, long position, long expiry, long length, ByteBuffer buffer){
    buffer.putInt((int) (locationId&0xffffffffL));
    buffer.putInt((int) (position&0xffffffffL));
    buffer.putInt((int) (expiry&0xffffffffL));
    buffer.putInt((int) (length&0xffffffffL));
  }

  public void readLong(Blackhole blackhole, ByteBuffer buffer){
    blackhole.consume( buffer.getLong());
    blackhole.consume( buffer.getLong());
    blackhole.consume( buffer.getLong());
    blackhole.consume( buffer.getLong());
  }

  public void writeLong(long locationId, long position, long expiry, long length, ByteBuffer buffer){
    buffer.putLong(locationId);
    buffer.putLong(position);
    buffer.putLong(expiry);
    buffer.putLong(length);
  }
}
