/*
 *   Copyright [2020 - 2022]   [Matthew Buckton]
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
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

import io.mapsmessaging.storage.impl.file.partition.archive.FileProcessor;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.DigestException;
import java.security.MessageDigest;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.jetbrains.annotations.NotNull;

public class FileCompressionProcessor implements FileProcessor {

  public long in(@Nonnull File in, @Nonnull @NotNull File out, @Nullable MessageDigest digest) throws IOException {
    long length = in.length();

    try(FileOutputStream fileOutputStream = new FileOutputStream(out)){
      try (FileInputStream fileInputStream = new FileInputStream(in)) {
        StreamCompressionHelper streamCompressionHelper = new StreamCompressionHelper();
        try {
          streamCompressionHelper.in(fileInputStream, fileOutputStream, digest);
        } catch (DigestException e) {
          throw new IOException(e);
        }
      }
    }
    return length;
  }

  public long out(@Nonnull File in, @Nonnull File out, @Nullable MessageDigest digest) throws IOException {
    try(FileInputStream fileInputStream = new FileInputStream(in)){
      try (FileOutputStream fileOutputStream = new FileOutputStream(out)) {
        StreamCompressionHelper streamCompressionHelper = new StreamCompressionHelper();
        try {
          streamCompressionHelper.out(fileInputStream, fileOutputStream, digest);
        } catch (DigestException e) {
          throw new IOException(e);
        }
      }
    }
    in.delete();
    return out.length();
  }
}
