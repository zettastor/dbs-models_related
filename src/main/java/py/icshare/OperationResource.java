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

/**
 * the resource which is related to operations.
 *
 */
public enum OperationResource {
  DOMAIN(1), STORAGEPOOL(2), VOLUME(3), DISK(4), SERVICE(5), ACCESSRULE(6), PERFORMANCETASK(
      7), ALARM(
      8), ALARMTEMPLATE(9), USER(10), LICENSE(11);
  private int value;

  private OperationResource(int value) {
    this.value = value;
  }

  public static OperationResource findByValue(int value) {
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
        return PERFORMANCETASK;
      case 8:
        return ALARM;
      case 9:
        return ALARMTEMPLATE;
      case 10:
        return USER;
      case 11:
        return LICENSE;
      default:
        return null;
    }
  }

}
