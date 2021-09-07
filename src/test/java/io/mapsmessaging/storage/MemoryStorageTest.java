package io.mapsmessaging.storage;

import io.mapsmessaging.storage.impl.MemoryStorage;
import java.io.InputStream;
import java.io.OutputStream;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

public class MemoryStorageTest {


  @Test
  public void simple(){
    Storage<StringStorable> simpleStore = new MemoryStorage<>(new StringStoreFactory());

  }

  public class StringStoreFactory implements Factory<StringStorable>{

    @Override
    public StringStorable create() {
      return new StringStorable();
    }
  }

  public static final class StringStorable implements Storable{

    @Override
    public long creation() {
      return 0;
    }

    @Override
    public long endOfLife() {
      return 0;
    }

    @Override
    public long lastRead() {
      return 0;
    }

    @Override
    public void read(@NotNull InputStream inputStream) {

    }

    @Override
    public void write(@NotNull OutputStream outputStream) {

    }
  }
}
