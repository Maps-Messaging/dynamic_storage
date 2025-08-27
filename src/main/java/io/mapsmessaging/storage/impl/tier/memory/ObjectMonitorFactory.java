/*
 *
 *  Copyright [ 2020 - 2024 ] Matthew Buckton
 *  Copyright [ 2024 - 2025 ] MapsMessaging B.V.
 *
 *  Licensed under the Apache License, Version 2.0 with the Commons Clause
 *  (the "License"); you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at:
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *      https://commonsclause.com/
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.mapsmessaging.storage.impl.tier.memory;

import io.mapsmessaging.storage.Storable;
import io.mapsmessaging.storage.StorableFactory;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;

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
