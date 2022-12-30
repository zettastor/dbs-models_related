/*
 * Copyright (c) 2022-2022. PengYunNetWork
 *
 * This program is free software: you can use, redistribute, and/or modify it
 * under the terms of the GNU Affero General Public License, version 3 or later ("AGPL"),
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *
 *  You should have received a copy of the GNU Affero General Public License along with
 *  this program. If not, see <http://www.gnu.org/licenses/>.
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
