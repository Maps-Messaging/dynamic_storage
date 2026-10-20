/*
 * Copyright [ 2024 - 2026 ] MapsMessaging B.V.
 */
package io.mapsmessaging.storage.impl.file.partition;

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.StorableFactory;
import java.nio.ByteBuffer;

final class RecoveryTestData {
  private RecoveryTestData() {}

  record Entry(long key, long expiry) implements Storable {
    public long getKey() { return key; }
    public long getExpiry() { return expiry; }
  }

  static final StorableFactory<Entry> FACTORY = new StorableFactory<>() {
    public ByteBuffer[] pack(Entry entry) {
      ByteBuffer buffer = ByteBuffer.allocate(16);
      buffer.putLong(entry.key()).putLong(entry.expiry()).flip();
      return new ByteBuffer[] {buffer};
    }

    public Entry unpack(ByteBuffer[] buffers) {
      return new Entry(buffers[0].getLong(), buffers[0].getLong());
    }
  };
}
