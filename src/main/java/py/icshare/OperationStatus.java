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

public enum OperationStatus {
  SUCCESS(1), FAILED(2), ACTIVITING(3);

  private static final Logger logger = LoggerFactory.getLogger(OperationStatus.class);
  private final int value;

  OperationStatus(int value) {
    this.value = value;
  }

  public static OperationStatus findByValue(int value) throws Exception {
    switch (value) {
      case 1:
        return SUCCESS;
      case 2:
        return FAILED;
      case 3:
        return ACTIVITING;
      default:
        logger.error("unknow value {}", value);
        throw new Exception();
    }
  }

  public static OperationStatus findByName(String operationStatusName) throws Exception {
    switch (operationStatusName) {
      case "SUCCESS":
        return SUCCESS;
      case "FAILED":
        return FAILED;
      case "ACTIVITING":
        return ACTIVITING;
      default:
        logger.error("unknow operationStatusName {}", operationStatusName);
        throw new Exception();
    }
  }

  public int getValue() {
    return value;
  }

  public boolean isEndStatus() throws Exception {
    switch (value) {
      case 1:
      case 2:
        return true;
      case 3:
        return false;
      default:
        logger.error("unknown value {}", value);
        throw new Exception();
    }
  }
}
