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
import io.mapsmessaging.storage.StorageFactory;
import io.mapsmessaging.storage.StorageFactoryFactory;
import java.util.LinkedHashMap;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SimpleFactoryCreationTest {


  @Test
  public void createInstances() {
    List<String> known = StorageFactoryFactory.getInstance().getKnown();
    Assertions.assertFalse(known.isEmpty());
    for(String test:known) {
      StorageFactory<StorableString> store = StorageFactoryFactory.getInstance().create(test, new LinkedHashMap<>(), new SimpleFactory());
      Assertions.assertNotNull(store);
      Assertions.assertEquals(store.getName(), test);
    }
  }

  static final class SimpleFactory implements Factory<StorableString> {

    @Override
    public StorableString create() {
      return new StorableString();
    }
  }

  static final class StorableString implements Storable {

    @Override
    public long getKey() {
      return 0;
    }

    @Override
    public void read(@NotNull ObjectReader objectReader) {

    }

    @Override
    public void write(@NotNull ObjectWriter objectWriter) {

    }
  }

}
