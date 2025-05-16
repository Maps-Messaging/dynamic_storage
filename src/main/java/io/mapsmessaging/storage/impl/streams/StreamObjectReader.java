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

package io.mapsmessaging.storage.impl.streams;

import java.io.IOException;
import java.io.InputStream;

public class StreamObjectReader extends ObjectReader {

  private final InputStream inputStream;

  public StreamObjectReader(InputStream inputStream) {
    this.inputStream = inputStream;
  }

  @Override
  public byte readByte() throws IOException {
    return (byte) (0xff & inputStream.read());
  }

  @Override
  public char readChar() throws IOException {
    return (char) readShort();
  }

  @Override

  protected long read(int size) throws IOException {
    return fromByteArray(readFromStream(size));
  }

  protected byte[] readFromStream(int length) throws IOException {
    byte[] result = null;
    if (length > -1) {
      result = new byte[length];
      int read = 0;
      while (read < length) {
        int t = inputStream.read(result, read, length - read);
        if (t < 0) {
          throw new IOException("End of stream encountered");
        }
        read += t;
      }
    }
    return result;
  }
}
