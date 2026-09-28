/*
 * Copyright [ 2024 - 2026 ] MapsMessaging B.V.
 */
package io.mapsmessaging.storage.impl.streams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class ObjectReaderReliabilityTest {

  @Test
  void rejectsNullByteArrayConversion() {
    TestObjectReader reader = new TestObjectReader();

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> reader.convert(null));

    assertEquals("Byte array must not be null", exception.getMessage());
  }

  @Test
  void convertsBytesAsBigEndianValue() {
    TestObjectReader reader = new TestObjectReader();

    assertEquals(0x0102FFL, reader.convert(new byte[] {0x01, 0x02, (byte) 0xFF}));
  }

  private static final class TestObjectReader extends ObjectReader {

    long convert(byte[] value) {
      return fromByteArray(value);
    }

    @Override
    protected byte[] readFromStream(int length) {
      return new byte[length];
    }

    @Override
    protected long read(int size) {
      return 0;
    }

    @Override
    public byte readByte() {
      return 0;
    }

    @Override
    public char readChar() {
      return 0;
    }
  }
}
