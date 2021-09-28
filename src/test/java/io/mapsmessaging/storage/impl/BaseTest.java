/*
 *
 * Copyright [2020 - 2021]   [Matthew Buckton]
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 *
 */

package io.mapsmessaging.storage.impl;

import io.mapsmessaging.storage.Factory;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.impl.streams.BufferObjectReader;
import io.mapsmessaging.storage.impl.streams.BufferObjectWriter;
import io.mapsmessaging.storage.impl.streams.ObjectReader;
import io.mapsmessaging.storage.impl.streams.ObjectWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;

public class BaseTest {
  private static final int BUFFER_SIZE = 1024;
  static{
    System.setProperty("PoolDepth", "64");
  }

  private static final byte CHAR = 1;
  private static final byte BYTE = 2;
  private static final byte SHORT = 3;
  private static final byte INT = 4;
  private static final byte LONG = 5;
  private static final byte FLOAT = 6;
  private static final byte DOUBLE = 7;
  private static final byte STRING = 8;

  private static final byte CHAR_ARRAY=11;
  private static final byte BYTE_ARRAY=12;
  private static final byte SHORT_ARRAY=13;
  private static final byte INT_ARRAY=14;
  private static final byte LONG_ARRAY=15;
  private static final byte FLOAT_ARRAY=16;
  private static final byte DOUBLE_ARRAY=17;
  private static final byte STRING_ARRAY=18;

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

    dataMapValues.put("byte_array", new byte[]{0x4, 0x66, 0xe});
    dataMapValues.put("char_array", new char[]{'f', 'f', '3','5','7'});
    dataMapValues.put("short_array", new short[]{32,321,543,543});
    dataMapValues.put("int_array", new int[]{423289048, 423782787, 342131212, 42343423});
    dataMapValues.put("long_array", new long[]{4232890472382789L, 42378278907L, 3421312312L, 423423423L});
    dataMapValues.put("float_array", new float[]{21.45f, 543.67f, 76.5f});
    dataMapValues.put("double_array", new double[]{12.4, 12234.553443, 231.2332});
    dataMapValues.put("string_array", new String[]{"String1", "string 2", "String 3"});
  }

  protected BaseStoreTest.MappedData createMessageBuilder(long id) {
    BaseStoreTest.MappedData mappedData = new BaseStoreTest.MappedData();
    mappedData.setMap(dataMapValues);
    mappedData.setKey(id);
    return mappedData;
  }

  protected void validateMessage(BaseStoreTest.MappedData message, long expectedId) {
    Assertions.assertNotNull(message);
    // Do the simple equivalence tests
    Assertions.assertEquals(expectedId, message.getKey(), "Identifier validation");
    validateDataMap(message.getMap());
  }

  protected void validateDataMap(Map<String, Object> dataMap) {
    Assertions.assertNotNull(dataMap, "This should not be a null value");
    for (Map.Entry<String, Object> entry : dataMapValues.entrySet()) {
      validateEntry(dataMap, entry.getKey(), entry.getValue());
    }
  }

  protected void validateEntry(Map<String, Object> dataMap, String key, Object value) {
    Object entry = dataMap.get(key);
    Assertions.assertNotNull(entry, "Entry test for " + key + " should not be empty");
    if(value instanceof byte[]) {
      Assertions.assertArrayEquals((byte[])entry, (byte[])value);
    }
    else if(value instanceof char[]) {
      Assertions.assertArrayEquals((char[])entry, (char[])value);
    }
    else if(value instanceof short[]) {
      Assertions.assertArrayEquals((short[])entry, (short[])value);
    }
    else if(value instanceof int[]) {
      Assertions.assertArrayEquals((int[])entry, (int[])value);
    }
    else if(value instanceof long[]) {
      Assertions.assertArrayEquals((long[])entry, (long[])value);
    }
    else if(value instanceof float[]) {
      Assertions.assertArrayEquals((float[])entry, (float[])value);
    }
    else if(value instanceof double[]) {
      Assertions.assertArrayEquals((double[])entry, (double[])value);
    }
    else if(value instanceof String[]) {
      Assertions.assertArrayEquals((String[])entry, (String[])value);
    }
    else {
      Assertions.assertEquals(value, entry);
    }
  }

  static byte[] build(){
    Random rdm = new Random(System.nanoTime());
    byte[] buf = new byte[BUFFER_SIZE];
    for(int x=0;x<BUFFER_SIZE;x++){
      buf[x] = (byte)(rdm.nextInt() % 0xff);
    }
    return buf;
  }

  static byte[] prebuilt = build();

  @ToString
  public static final class MappedData implements Storable {
    @Getter @Setter long key;
    @Getter @Setter Map<String, Object> map;
    @Getter @Setter ByteBuffer data;

    public MappedData(){
      byte[] buf = build();
      data = ByteBuffer.allocate(buf.length);
      data.put(buf);
      data.flip();
    }
    @Override
    public void read(@NotNull ByteBuffer[] buffers) throws IOException {
      BufferObjectReader bor = new BufferObjectReader(buffers[0]);
      readHeader(bor);
      readMap(bor);
      data = buffers[1];
    }

    void readHeader(@NotNull ObjectReader objectReader) throws IOException {
      key = objectReader.readLong();
    }

    void readMap(@NotNull ObjectReader objectReader) throws IOException {
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
          case CHAR_ARRAY:
            map.put(key, objectReader.readCharArray());
            break;
          case BYTE_ARRAY:
            map.put(key, objectReader.readByteArray());
            break;
          case SHORT_ARRAY:
            map.put(key, objectReader.readShortArray());
            break;
          case INT_ARRAY:
            map.put(key, objectReader.readIntArray());
            break;
          case LONG_ARRAY:
            map.put(key, objectReader.readLongArray());
            break;
          case FLOAT_ARRAY:
            map.put(key, objectReader.readFloatArray());
            break;
          case DOUBLE_ARRAY:
            map.put(key, objectReader.readDoubleArray());
            break;
          case STRING_ARRAY:
            map.put(key, objectReader.readStringArray());
            break;
        }
      }
    }

    void writeHeader(@NotNull ObjectWriter objectWriter) throws IOException {
      objectWriter.write(key);
    }

    void writeMap(@NotNull ObjectWriter objectWriter) throws IOException {
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
        else if(obj instanceof char[]){
          objectWriter.write(CHAR_ARRAY);
          objectWriter.write((char[])obj);
        }
        else if(obj instanceof byte[]){
          objectWriter.write(BYTE_ARRAY);
          objectWriter.write((byte[])obj);
        }
        else if(obj instanceof short[]){
          objectWriter.write(SHORT_ARRAY);
          objectWriter.write((short[])obj);
        }
        else if(obj instanceof int[]){
          objectWriter.write(INT_ARRAY);
          objectWriter.write((int[])obj);
        }
        else if(obj instanceof long[]){
          objectWriter.write(LONG_ARRAY);
          objectWriter.write((long[])obj);
        }
        else if(obj instanceof float[]){
          objectWriter.write(FLOAT_ARRAY);
          objectWriter.write((float[])obj);
        }
        else if(obj instanceof double[]){
          objectWriter.write(DOUBLE_ARRAY);
          objectWriter.write((double[])obj);
        }
        else if(obj instanceof String[]){
          objectWriter.write(STRING_ARRAY);
          objectWriter.write((String[])obj);
        }
      }
    }


    @Override
    public @NotNull ByteBuffer[] write() throws IOException {
      ByteBuffer[] packed = new ByteBuffer[3];
      ByteBuffer headers = ByteBuffer.allocate(10240);
      BufferObjectWriter bow = new BufferObjectWriter(headers);
      writeHeader(bow);
      writeMap(bow);
      headers.flip();
      packed[0] = headers;
      ByteBuffer buf = ByteBuffer.allocate(prebuilt.length);
      buf.put(prebuilt);
      buf.flip();
      packed[1] = buf;
      packed[2] = data;
      return packed;
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
