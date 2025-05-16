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

package io.mapsmessaging.storage.impl;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class S3Helper {

  public static Properties getProperties() throws IOException {
    Properties prop = new Properties();
    try (InputStream input = S3Helper.class.getClassLoader().getResourceAsStream("s3.properties")) {
      if (input != null) {
        prop.load(input);
        return prop;
      }
    }
    try (InputStream input = new FileInputStream("s3.properties")) {
      prop.load(input);
      return prop;
    }
    catch(FileNotFoundException ex){
      // ignore this since we can fall through
    }
    return System.getProperties();
  }

}
