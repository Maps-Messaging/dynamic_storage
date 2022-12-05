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

package io.mapsmessaging.storage.impl.file.partition.archive;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.util.Arrays;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class StreamProcessor {

  public int in(@Nonnull InputStream inputStream, @Nonnull  OutputStream outputStream, @Nullable MessageDigest messageDigest) throws IOException{
    return processStreams(inputStream, outputStream, messageDigest);
  }

  public int out(@Nonnull InputStream inputStream, @Nonnull OutputStream outputStream, @Nullable MessageDigest messageDigest) throws IOException {
    return processStreams(inputStream, outputStream, messageDigest);
  }

  protected int processStreams(@Nonnull InputStream inputStream, @Nonnull OutputStream outputStream, @Nullable MessageDigest messageDigest) throws IOException {
    if(messageDigest == null){
      return joinStreams(inputStream, outputStream);
    }
    return joinStreams(inputStream, outputStream, messageDigest);
  }

  private int joinStreams(@Nonnull InputStream inputStream, @Nonnull OutputStream outputStream) throws IOException {
    byte[] tmp = new byte[10240];
    int count = 0;
    int len =1;
    while (len >0) {
      len = inputStream.read(tmp, 0, tmp.length);
      if (len > 0 ) {
        outputStream.write(tmp, 0, len);
        count += len;
      }
    }
    outputStream.flush();
    return count;
  }

  private int joinStreams(@Nonnull InputStream inputStream, @Nonnull OutputStream outputStream, @Nonnull MessageDigest messageDigest) throws IOException{
    byte[] tmp = new byte[10240];
    int count = 0;
    int len = 0;
    int pos =0;
    int bufLen = tmp.length;
    int digestLength = messageDigest.getDigestLength();
    while (len >= 0) {
      len += inputStream.read(tmp, pos, bufLen);
      if (len > 0 ) {
        if(len > digestLength){
          outputStream.write(tmp, 0, len);
          messageDigest.update(tmp, 0, len);
          count += len;
          len =0;
          pos =0;
          bufLen = tmp.length;
          Arrays.fill( tmp, (byte) 0 );
        }
        else if(inputStream.available() > 0){
          pos = len;
          bufLen = bufLen -len;
        }
      }
    }
    outputStream.flush();
    return count;
  }
}
