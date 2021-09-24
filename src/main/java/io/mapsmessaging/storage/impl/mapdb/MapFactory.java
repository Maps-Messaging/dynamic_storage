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

package io.mapsmessaging.storage.impl.mapdb;

import io.mapsmessaging.storage.Factory;
import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.Storage;
import io.mapsmessaging.storage.impl.BaseStorageFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MapFactory<T extends Storable> extends BaseStorageFactory<T> {

  public MapFactory() {
  }

  public MapFactory(Map<String, String> properties, Factory<T> factory) {
    super(properties, factory);
  }


  @Override
  public String getName() {
    return "MapDB";
  }

  @Override
  public Storage<T> create(String name) {
    boolean sync = false;
    if (properties.containsKey("Sync")) {
      sync = Boolean.parseBoolean(properties.get("Sync"));
    }
    return new MapStorage<>(properties.get("basePath"), name, factory, sync);
  }

  @Override
  public List<Storage<T>> discovered() {
    return new ArrayList<>();
  }

}