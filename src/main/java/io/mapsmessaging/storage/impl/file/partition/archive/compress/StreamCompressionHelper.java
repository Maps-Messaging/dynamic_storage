/*
 *    Copyright [ 2020 - 2024 ] [Matthew Buckton]
 *    Copyright [ 2024 - 2025 ] [Maps Messaging B.V.]
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 */

package io.mapsmessaging.storage.impl.file.partition.archive.compress;

import io.mapsmessaging.storage.impl.file.partition.archive.StreamProcessor;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class StreamCompressionHelper extends StreamProcessor {

  @Override
  public int in(@NotNull InputStream inputStream, @NotNull OutputStream outputStream, @Nullable MessageDigest messageDigest) throws IOException {
    try(GZIPOutputStream gzipOutputStream = new GZIPOutputStream(outputStream)) {
      return super.in(inputStream, gzipOutputStream, messageDigest);
    }
  }

  @Override
  public int out(@NotNull InputStream inputStream, @NotNull OutputStream outputStream, @Nullable MessageDigest messageDigest) throws IOException {
    try(GZIPInputStream gzipInputStream = new GZIPInputStream(inputStream)) {
      return super.out(gzipInputStream, outputStream, messageDigest);
    }
  }
}
