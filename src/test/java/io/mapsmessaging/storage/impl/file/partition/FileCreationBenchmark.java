package io.mapsmessaging.storage.impl.file.partition;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.SPARSE;
import static java.nio.file.StandardOpenOption.WRITE;

import io.mapsmessaging.utilities.collections.MappedBufferHelper;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;

@State(Scope.Benchmark)
public class FileCreationBenchmark {

  private static final int ITEM_SIZE = 1024 * 1024; // 1 million events
  private static final int BUF_SIZE = ITEM_SIZE * IndexRecord.HEADER_SIZE;
  private static final File file = new File("./test.bin");

  @Benchmark
  @BenchmarkMode({Mode.Throughput})
  @Fork(value = 1, warmups = 2)
  @Threads(1)
  public void existingAlgorithm() throws IOException {
    StandardOpenOption[] writeOptions = new StandardOpenOption[]{CREATE, READ, WRITE, SPARSE};
    try (FileChannel mapChannel = (FileChannel) Files.newByteChannel(file.toPath(), writeOptions)) {
      IndexRecord empty = new IndexRecord();
      MappedByteBuffer index = mapChannel.map(FileChannel.MapMode.READ_WRITE, 0, BUF_SIZE);
      index.load(); // Ensure the file contents are loaded
      for (int x = 0; x < ITEM_SIZE; x++) {
        empty.update(index); // fill with 0's
      }
      MappedBufferHelper.closeDirectBuffer(index);
    }
    Files.deleteIfExists(file.toPath());
  }

  @Benchmark
  @BenchmarkMode({Mode.Throughput})
  @Fork(value = 1, warmups = 2)
  @Threads(1)
  public void existingAlgorithmMk2() throws IOException {
    StandardOpenOption[] writeOptions = new StandardOpenOption[]{CREATE, READ, WRITE, SPARSE};
    try (FileChannel mapChannel = (FileChannel) Files.newByteChannel(file.toPath(), writeOptions)) {
      MappedByteBuffer index = mapChannel.map(FileChannel.MapMode.READ_WRITE, 0, BUF_SIZE);
      index.load(); // Ensure the file contents are loaded
      byte[] buf = new byte[BUF_SIZE];
      index.put(buf);
      MappedBufferHelper.closeDirectBuffer(index);
    }
    Files.deleteIfExists(file.toPath());
  }

  @Benchmark
  @BenchmarkMode({Mode.Throughput})
  @Fork(value = 1, warmups = 2)
  @Threads(1)
  public void byteFillAlgorithm() throws IOException {
    StandardOpenOption[] writeOptions = new StandardOpenOption[]{CREATE, READ, WRITE, SPARSE};
    try (FileChannel mapChannel = (FileChannel) Files.newByteChannel(file.toPath(), writeOptions)) {
      byte[] buf = new byte[BUF_SIZE];
      ByteBuffer bb = ByteBuffer.wrap(buf);
      mapChannel.write(bb);
    }
    Files.deleteIfExists(file.toPath());
  }

  @Benchmark
  @BenchmarkMode({Mode.Throughput})
  @Fork(value = 1, warmups = 2)
  @Threads(1)
  public void byteFillDirectAlgorithm() throws IOException {
    StandardOpenOption[] writeOptions = new StandardOpenOption[]{CREATE, READ, WRITE, SPARSE};
    try (FileChannel mapChannel = (FileChannel) Files.newByteChannel(file.toPath(), writeOptions)) {
      mapChannel.write(ByteBuffer.allocateDirect(BUF_SIZE));
    }
    Files.deleteIfExists(file.toPath());
  }

  @Benchmark
  @BenchmarkMode({Mode.Throughput})
  @Fork(value = 1, warmups = 2)
  @Threads(1)
  public void setPosition() throws IOException {
    StandardOpenOption[] writeOptions = new StandardOpenOption[]{CREATE, READ, WRITE, SPARSE};
    try (FileChannel mapChannel = (FileChannel) Files.newByteChannel(file.toPath(), writeOptions)) {
      mapChannel.position(BUF_SIZE - 1);
      // Sets the position to the size we want -1, and then we write a byte
      byte[] buf = new byte[1];
      ByteBuffer bb = ByteBuffer.wrap(buf);
      mapChannel.write(bb);
    }
    Files.deleteIfExists(file.toPath());
  }

}
