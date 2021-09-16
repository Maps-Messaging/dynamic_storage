package io.mapsmessaging.storage.impl.mapDb;

import io.mapsmessaging.storage.Factory;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.impl.DataObjectReader;
import io.mapsmessaging.storage.impl.ObjectReader;
import io.mapsmessaging.storage.impl.ObjectWriter;
import io.mapsmessaging.storage.impl.StreamObjectWriter;
import java.io.IOException;
import org.jetbrains.annotations.NotNull;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.serializer.GroupSerializerObjectArray;

public class MapDBSerializer<T extends Storable> extends GroupSerializerObjectArray<T> {

  private final Factory<T> factory;

  public MapDBSerializer(Factory<T> factory) {
    this.factory = factory;
  }

  public void serialize(@NotNull DataOutput2 out, T context) throws IOException {
    ObjectWriter writer = new StreamObjectWriter(out);
    context.write(writer);
  }

  public T deserialize(@NotNull DataInput2 in, int available) throws IOException {
    ObjectReader reader = new DataObjectReader(in);
    try {
      T obj = factory.create();
      obj.read(reader);
      return obj;
    } catch (Exception e) {
      throw new IOException("Unable to construct serialized object", e);
    }
  }

  @Override
  public boolean isTrusted() {
    return true;
  }
}