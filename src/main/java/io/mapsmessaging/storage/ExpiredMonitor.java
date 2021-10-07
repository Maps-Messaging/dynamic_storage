package io.mapsmessaging.storage;

import java.io.IOException;

public interface ExpiredMonitor {

  void scanForExpired() throws IOException;

}
