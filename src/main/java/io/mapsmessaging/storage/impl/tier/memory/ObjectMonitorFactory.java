package io.mapsmessaging.storage.impl.tier.memory;

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.StorableFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.jetbrains.annotations.NotNull;

public class ObjectMonitorFactory<T extends Storable> implements StorableFactory<ObjectMonitor<T>> {

  @Override
  public @NotNull ObjectMonitor<T> unpack(@NotNull ByteBuffer[] reloadBuffers) throws IOException {
    return new ObjectMonitor<>();
  }

  @Override
  public @NotNull ByteBuffer[] pack(@NotNull ObjectMonitor<T> object) throws IOException {
    return new ByteBuffer[0];
  }
}
