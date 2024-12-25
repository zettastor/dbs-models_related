/**
* Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/ 

package py.common;

import java.io.IOException;
import java.net.Socket;
import org.slf4j.LoggerFactory;

public enum PortUtil {
  INSTANCE;

  private static final int MAX_PORT = 65535;
  private static final int MIN_PORT = 10000;
  private static final int MAX_TRY_COUNT = 10000;
  protected static org.slf4j.Logger logger = LoggerFactory.getLogger(PortUtil.class);
  private int currentPort = MIN_PORT;

  /**
   * 端口是否可以占用.
   */
  public static boolean isLocalPortAllocatable(int portNum) {
    Socket socket = null;
    try {
      socket = new Socket("127.0.0.1", portNum);
      return false;
    } catch (IOException e) {
      logger.debug(String.format("Port:[%d] is in used.", portNum));

      return true;
    } finally {
      if (socket != null) {
        try {
          socket.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  public int allocatePort() {
    int newPort = allocatePort(currentPort + 1);
    currentPort = newPort;

    return currentPort;
  }

  /**
   * find an available port.
   */
  public int allocatePort(int beginPort) {
    for (int i = beginPort; i < beginPort + MAX_TRY_COUNT; ++i) {
      int portNum = i > MAX_PORT ? i % MAX_PORT + MIN_PORT : i;
      if (isLocalPortAllocatable(portNum)) {
        return portNum;
      }
    }

    throw new RuntimeException("Cann't find idle port.");
  }
}
