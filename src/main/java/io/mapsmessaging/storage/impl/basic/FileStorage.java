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

package io.mapsmessaging.storage.impl.basic;

import io.mapsmessaging.storage.Factory;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.impl.BaseIndexStorage;
import io.mapsmessaging.storage.impl.RandomAccessFileObjectReader;
import io.mapsmessaging.storage.impl.RandomAccessFileObjectWriter;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import org.jetbrains.annotations.NotNull;

public class FileStorage<T extends Storable> extends BaseIndexStorage<T> {

  private final RandomAccessFile randomAccessWriteFile;
  private final RandomAccessFile randomAccessReadFile;
  private final RandomAccessFileObjectWriter writer;
  private final RandomAccessFileObjectReader reader;
  private final String fileName;
  private final Factory<T> factory;
  private final boolean sync;

  public FileStorage(String fileName, Factory<T> factory, boolean sync) throws IOException {

    this.fileName = fileName;
    this.factory = factory;
    randomAccessWriteFile = new RandomAccessFile(fileName, "rw");
    randomAccessReadFile = new RandomAccessFile(fileName, "rw");
    writer = new RandomAccessFileObjectWriter(randomAccessWriteFile);
    reader = new RandomAccessFileObjectReader(randomAccessReadFile);
    if (randomAccessReadFile.length() != 0) {
      reload();
      randomAccessWriteFile.seek(randomAccessReadFile.getFilePointer());
    }
    this.sync = sync;
  }

  private void reload() throws IOException {
    long pos = 0;
    long eof = randomAccessReadFile.length();
    while (pos != eof) {
      T entry = factory.create();
      entry.read(reader);
      index.put(entry.getKey(), pos);
      pos = randomAccessReadFile.getFilePointer();
    }
  }

  @Override
  public void close() throws IOException {
    randomAccessWriteFile.close();
    randomAccessReadFile.close();
  }

  @Override
  public String getName() {
    return fileName;
  }

  @Override
  public void delete() throws IOException {
    randomAccessWriteFile.close();
    randomAccessReadFile.close();
    File file = new File(getName());
    Files.delete(file.toPath());
  }

  @Override
  public void add(@NotNull T entry) throws IOException {
    long pos = randomAccessWriteFile.getFilePointer();
    randomAccessWriteFile.seek(pos);
    entry.write(writer);
    index.put(entry.getKey(), pos);
    if (sync) {
      randomAccessReadFile.getChannel().force(false);
    }
  }

  @Override
  public T get(long key) throws IOException {
    T entry = null;
    if (key >= 0) {
      Long pos = index.get(key);
      if (pos != null) {
        randomAccessReadFile.seek(pos);
        entry = factory.create();
        entry.read(reader);
      }
    }
    return entry;
  }

  @Override
  public boolean remove(long key) throws IOException {
    Long pos = index.remove(key);
    if (pos != null) {
      randomAccessReadFile.seek(pos);
      randomAccessReadFile.writeLong(-1);
      if (sync) {
        randomAccessReadFile.getChannel().force(false);
      }
    }
    return pos != null;
  }
}
