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
