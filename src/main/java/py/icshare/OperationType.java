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
