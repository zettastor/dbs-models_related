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

package py.icshare;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum OperationType {
  CREATE(1), DELETE(2), EXTEND(3), CYCLE(5), LAUNCH(6), UMOUNT(7),
  APPLY(9), CANCEL(10), MODIFY(11), LOGIN(12), LOGOUT(13), RESET(15), ONLINE(16),
  OFFLINE(17), ASSIGN(18), MIGRATE(21), REBALANCE(22);
  private static final Logger logger = LoggerFactory.getLogger(OperationType.class);
  private final int value;

  OperationType(int value) {
    this.value = value;
  }

  public static OperationType findByValue(int value) throws Exception {
    switch (value) {
      case 1:
        return CREATE;
      case 2:
        return DELETE;
      case 3:
        return EXTEND;
      case 5:
        return CYCLE;
      case 6:
        return LAUNCH;
      case 7:
        return UMOUNT;
      case 9:
        return APPLY;
      case 10:
        return CANCEL;
      case 11:
        return MODIFY;
      case 12:
        return LOGIN;
      case 13:
        return LOGOUT;
      case 15:
        return RESET;
      case 16:
        return ONLINE;
      case 17:
        return OFFLINE;
      case 18:
        return ASSIGN;
      case 21:
        return MIGRATE;
      case 22:
        return REBALANCE;

      default:
        logger.error("unknown value {}", value);
        throw new Exception();
    }
  }

  public static OperationType findByName(String operationTypeName) throws Exception {
    switch (operationTypeName) {
      case "CREATE":
        return CREATE;
      case "DELETE":
        return DELETE;
      case "EXTEND":
        return EXTEND;
      case "CYCLE":
        return CYCLE;
      case "LAUNCH":
        return LAUNCH;
      case "AMOUNT":
        return UMOUNT;
      case "APPLY":
        return APPLY;
      case "CANCEL":
        return CANCEL;
      case "MODIFY":
        return MODIFY;
      case "LOGIN":
        return LOGIN;
      case "LOGOUT":
        return LOGOUT;
      case "RESET":
        return RESET;
      case "ONLINE":
        return ONLINE;
      case "OFFLINE":
        return OFFLINE;
      case "ASSIGN":
        return ASSIGN;
      case "MIGRATE":
        return MIGRATE;
      case "REBALANCE":
        return REBALANCE;
      default:
        logger.error("unknown operationTypeName {}", operationTypeName);
        throw new Exception();
    }
  }

  public int getValue() {
    return value;
  }
}
