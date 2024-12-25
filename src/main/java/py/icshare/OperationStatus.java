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
