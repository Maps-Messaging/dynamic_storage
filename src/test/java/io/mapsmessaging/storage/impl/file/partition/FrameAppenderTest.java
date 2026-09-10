package io.mapsmessaging.storage.impl.file.partition;

import org.junit.jupiter.api.Test;

class FrameAppenderTest {
  @Test
  void completeWritesRollbackAndLegacyFrames() throws Exception {
    FrameAppenderChecks.main(new String[0]);
  }
}
