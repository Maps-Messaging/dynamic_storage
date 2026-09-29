/*
 * Copyright [ 2024 - 2026 ] MapsMessaging B.V.
 */
package io.mapsmessaging.storage.impl.streams;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.EOFException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RandomAccessFileObjectReaderTest {

  @TempDir
  Path tempDir;

  @Test
  void readsByteArrayWithoutMutatingFile() throws Exception {
    Path file = tempDir.resolve("byte-array.bin");
    byte[] payload = {1, 2, 3, 4};

    try (RandomAccessFile randomAccessFile = new RandomAccessFile(file.toFile(), "rw")) {
      randomAccessFile.writeInt(payload.length);
      randomAccessFile.write(payload);
      long originalLength = randomAccessFile.length();

      randomAccessFile.seek(0);
      RandomAccessFileObjectReader reader = new RandomAccessFileObjectReader(randomAccessFile);

      assertArrayEquals(payload, reader.readByteArray());
      assertEquals(originalLength, randomAccessFile.length());
    }
  }

  @Test
  void readsStringThroughFullyReadPayloadPath() throws Exception {
    Path file = tempDir.resolve("string.bin");
    byte[] payload = "dynamic-storage".getBytes();

    try (RandomAccessFile randomAccessFile = new RandomAccessFile(file.toFile(), "rw")) {
      randomAccessFile.writeInt(payload.length);
      randomAccessFile.write(payload);

      randomAccessFile.seek(0);
      RandomAccessFileObjectReader reader = new RandomAccessFileObjectReader(randomAccessFile);

      assertEquals("dynamic-storage", reader.readString());
    }
  }

  @Test
  void truncatedByteArrayFailsInsteadOfReturningPartialData() throws Exception {
    Path file = tempDir.resolve("truncated.bin");

    try (RandomAccessFile randomAccessFile = new RandomAccessFile(file.toFile(), "rw")) {
      randomAccessFile.writeInt(4);
      randomAccessFile.write(new byte[] {1, 2});

      randomAccessFile.seek(0);
      RandomAccessFileObjectReader reader = new RandomAccessFileObjectReader(randomAccessFile);

      assertThrows(EOFException.class, reader::readByteArray);
    }
  }

  @Test
  void nullByteArrayEncodingRemainsNull() throws Exception {
    Path file = tempDir.resolve("null-array.bin");

    try (RandomAccessFile randomAccessFile = new RandomAccessFile(file.toFile(), "rw")) {
      randomAccessFile.writeInt(-1);

      randomAccessFile.seek(0);
      RandomAccessFileObjectReader reader = new RandomAccessFileObjectReader(randomAccessFile);

      assertEquals(null, reader.readByteArray());
    }
  }
}
