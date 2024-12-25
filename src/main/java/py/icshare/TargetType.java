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

/**
 * the resource which is related to operations.
 *
 */
public enum TargetType {
  DOMAIN(1), STORAGEPOOL(2), VOLUME(3), DISK(4), SERVICE(5), ACCESSRULE(6), ALARM(
      7), ALARMTEMPLATE(8), USER(9), DRIVER(11), ROLE(13), PASSWORD(14),
  QOS(15), RESOURCE(16), SERVERNODE(17), SCSI_CLIENT(19), REBALANCE(20);
  private static final Logger logger = LoggerFactory.getLogger(OperationStatus.class);
  private int value;

  private TargetType(int value) {
    this.value = value;
  }

  public static TargetType findByValue(int value) throws Exception {
    switch (value) {
      case 1:
        return DOMAIN;
      case 2:
        return STORAGEPOOL;
      case 3:
        return VOLUME;
      case 4:
        return DISK;
      case 5:
        return SERVICE;
      case 6:
        return ACCESSRULE;
      case 7:
        return ALARM;
      case 8:
        return ALARMTEMPLATE;
      case 9:
        return USER;
      case 11:
        return DRIVER;
      case 13:
        return ROLE;
      case 14:
        return PASSWORD;
      case 15:
        return QOS;
      case 16:
        return RESOURCE;
      case 17:
        return SERVERNODE;
      case 19:
        return SCSI_CLIENT;
      case 20:
        return REBALANCE;
      default:
        logger.error("unknown value {}", value);
        throw new Exception();
    }
  }

  public static TargetType findByName(String operationTargetName) throws Exception {
    switch (operationTargetName) {
      case "DOMAIN":
        return DOMAIN;
      case "STORAGEPOOL":
        return STORAGEPOOL;
      case "VOLUME":
        return VOLUME;
      case "DISK":
        return DISK;
      case "SERVICE":
        return SERVICE;
      case "ACCESSRULE":
        return ACCESSRULE;
      case "ALARM":
        return ALARM;
      case "ALARMTEMPLATE":
        return ALARMTEMPLATE;
      case "USER":
        return USER;
      case "DRIVER":
        return DRIVER;
      case "ROLE":
        return ROLE;
      case "PASSWORD":
        return PASSWORD;
      case "QOS":
        return QOS;
      case "RESOURCE":
        return RESOURCE;
      case "SERVERNODE":
        return SERVERNODE;
      case "SCSI_CLIENT":
        return SCSI_CLIENT;
      case "REBALANCE":
        return REBALANCE;
      default:
        logger.error("unknown operationTargetName {}", operationTargetName);
        throw new Exception();
    }
  }

  public int getValue() {
    return value;
  }

}
