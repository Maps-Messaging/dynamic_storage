package io.mapsmessaging.storage.impl;

import io.mapsmessaging.storage.Factory;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.utilities.threads.tasks.ThreadLocalContext;
import io.mapsmessaging.utilities.threads.tasks.ThreadStateContext;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public abstract class BaseStoreTest {

  private static final byte CHAR = 1;
  private static final byte BYTE = 2;
  private static final byte SHORT = 3;
  private static final byte INT = 4;
  private static final byte LONG = 5;
  private static final byte FLOAT = 6;
  private static final byte DOUBLE = 7;
  private static final byte STRING = 8;

  private static final Map<String, Object> dataMapValues;
  static {
    dataMapValues = new LinkedHashMap<>();
    dataMapValues.put("low_long", Long.MIN_VALUE);
    dataMapValues.put("high_long", Long.MAX_VALUE);

    dataMapValues.put("low_float", Float.MIN_VALUE);
    dataMapValues.put("high_float", Float.MAX_VALUE);

    dataMapValues.put("low_int", Integer.MIN_VALUE);
    dataMapValues.put("high_int", Integer.MAX_VALUE);

    dataMapValues.put("low_double", Double.MIN_VALUE);
    dataMapValues.put("high_double", Double.MAX_VALUE);

    dataMapValues.put("low_byte", Byte.MIN_VALUE);
    dataMapValues.put("byte", (byte) 0xff);
    dataMapValues.put("high_byte", Byte.MAX_VALUE);

    dataMapValues.put("low_short", Short.MIN_VALUE);
    dataMapValues.put("short", (short) 0xFFFF);
    dataMapValues.put("high_short", Short.MAX_VALUE);

    dataMapValues.put("string", "ABC");

    dataMapValues.put("low_char", Character.MIN_VALUE);
    dataMapValues.put("char", 'c');
    dataMapValues.put("japanese", '大');
    dataMapValues.put("high_char", Character.MAX_VALUE);
  }

  public abstract Storage<MappedData> createStore(boolean sync) throws IOException;


  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void basicIOTestWithAndWithoutSync(boolean sync) throws IOException {
    Storage<MappedData> storage = null;
    try {
      storage = createStore(sync);
      ThreadStateContext context = new ThreadStateContext();
      context.add("domain", "ResourceAccessKey");
      ThreadLocalContext.set(context);
      // Remove any before we start

      for (int x = 0; x < 10; x++) {
        MappedData message = createMessageBuilder(x);
        validateMessage(message, x);
        storage.add(message);
      }
      Assertions.assertEquals(10, storage.size());

      for (int x = 0; x < 10; x++) {
        MappedData message = storage.get(x);
        validateMessage(message, x);
        storage.remove(x);
        System.err.println(message.toString());
      }
      Assertions.assertTrue(storage.isEmpty());
    } finally {
      if(storage != null) {
        storage.delete();
      }
    }
  }

  private MappedData createMessageBuilder(long id) {
    MappedData mappedData = new MappedData();
    mappedData.setMap(dataMapValues);
    mappedData.setKey(id);
    return mappedData;
  }

  private void validateMessage(MappedData message, long expectedId) {
    Assertions.assertNotNull(message);
    // Do the simple equivalence tests
    Assertions.assertEquals(expectedId, message.getKey(), "Identifier validation");
    validateDataMap(message.getMap());


  }

  private void validateDataMap(Map<String, Object> dataMap) {
    Assertions.assertNotNull(dataMap, "This should not be a null value");
    for (Map.Entry<String, Object> entry : dataMapValues.entrySet()) {
      validateEntry(dataMap, entry.getKey(), entry.getValue());
    }
  }

  private void validateEntry(Map<String, Object> dataMap, String key, Object value) {
    Object entry = dataMap.get(key);
    Assertions.assertNotNull(entry, "Entry test for " + key + " should not be empty");
    Assertions.assertEquals(value, entry);
  }


  @ToString
  public static final class MappedData implements Storable {
    @Getter @Setter long key;
    @Getter @Setter Map<String, Object> map;

    @Override
    public void read(@NotNull ObjectReader objectReader) throws IOException {
      key = objectReader.readLong();
      map = new LinkedHashMap<>();
      int size = objectReader.readInt();
      for(int x=0;x<size;x++){
        String key = objectReader.readString();
        byte type = objectReader.readByte();
        switch (type){
          case CHAR:
            map.put(key, objectReader.readChar());
            break;

          case BYTE:
            map.put(key, objectReader.readByte());
            break;
          case SHORT:
            map.put(key, objectReader.readShort());
            break;
          case INT:
            map.put(key, objectReader.readInt());
            break;

          case LONG:
            map.put(key, objectReader.readLong());
            break;

          case FLOAT:
            map.put(key, objectReader.readFloat());
            break;

          case DOUBLE:
            map.put(key, objectReader.readDouble());
            break;

          case STRING:
            map.put(key, objectReader.readString());
            break;
        }
      }
    }

    @Override
    public void write(@NotNull ObjectWriter objectWriter) throws IOException {
      objectWriter.write(key);
      objectWriter.write(map.size());
      for(String key:map.keySet()){
        objectWriter.write(key);
        Object obj = map.get(key);
        if(obj instanceof Byte){
          objectWriter.write(BYTE);
          objectWriter.write((byte)obj);
        }
        else if(obj instanceof Character){
          objectWriter.write(CHAR);
          objectWriter.write((Character)obj);
        }
        else if(obj instanceof Short){
          objectWriter.write(SHORT);
          objectWriter.write((Short)obj);
        }
        else if(obj instanceof Integer){
          objectWriter.write(INT);
          objectWriter.write((Integer)obj);
        }
        else if(obj instanceof Long){
          objectWriter.write(LONG);
          objectWriter.write((Long)obj);
        }
        else if(obj instanceof Float){
          objectWriter.write(FLOAT);
          objectWriter.write((Float)obj);
        }
        else if(obj instanceof Double){
          objectWriter.write(DOUBLE);
          objectWriter.write((Double)obj);
        }
        else if(obj instanceof String){
          objectWriter.write(STRING);
          objectWriter.write((String)obj);
        }
      }
    }
  }

  public Factory<MappedData> getFactory(){
    return new MappedDataFactory();
  }

  public static final class MappedDataFactory implements Factory<MappedData>{

    @Override
    public MappedData create() {
      return new MappedData();
    }
  }

}