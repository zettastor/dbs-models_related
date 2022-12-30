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

package py.icshare.authorization;

import java.util.Objects;

public class ApiToAuthorize {
  private String apiName;
  private String category;
  private String chineseText;
  private String englishText;

  public ApiToAuthorize() {
  }

  public ApiToAuthorize(String apiName) {
    this.apiName = apiName;
    this.category = ApiCategory.Other.name();
  }

  public ApiToAuthorize(String apiName, String category) {
    this.apiName = apiName;
    this.category = category;
  }

  public ApiToAuthorize(String apiName, String category, String chineseText, String englishText) {
    this.apiName = apiName;
    this.category = category;
    this.chineseText = chineseText;
    this.englishText = englishText;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ApiToAuthorize that = (ApiToAuthorize) o;
    return Objects.equals(apiName, that.apiName);
  }

  @Override
  public String toString() {
    return "APIToAuthorize{" + "apiName='" + apiName + '\'' + ", category='" + category + '\''
        + '}';
  }

  @Override
  public int hashCode() {
    return Objects.hash(apiName);
  }

  public String getApiName() {
    return apiName;
  }

  public void setApiName(String apiName) {
    this.apiName = apiName;
  }

  public String getCategory() {
    return category;
  }

  public void setCategory(String category) {
    this.category = category;
  }

  public String getChineseText() {
    return chineseText;
  }

  public void setChineseText(String chineseText) {
    this.chineseText = chineseText;
  }

  public String getEnglishText() {
    return englishText;
  }

  public void setEnglishText(String englishText) {
    this.englishText = englishText;
  }

  public enum ApiCategory {
    Volume(1),
    StoragePool(2),
    Domain(3),
    Driver(4),
    Access_Rule(5),
    Hardware(6),
    Account(7),
    Role(8),
    License(9),
    QOS(10),
    PerformanceTask(11),
    PerformanceData(12),
    AlertTemplate(13),
    AlertMessage(14),
    AlertFoward(15),
    SnapShot(16),
    Other(17);

    private final int value;

    ApiCategory(int value) {
      this.value = value;
    }

    public static ApiCategory findByValue(int value) {
      switch (value) {
        case 1:
          return Volume;
        case 2:
          return StoragePool;
        case 3:
          return Domain;
        case 4:
          return Driver;
        case 5:
          return Access_Rule;
        case 6:
          return Hardware;
        case 7:
          return Account;
        case 8:
          return Role;
        case 9:
          return License;
        case 10:
          return QOS;
        case 11:
          return PerformanceTask;
        case 12:
          return PerformanceData;
        case 13:
          return AlertTemplate;
        case 14:
          return AlertMessage;
        case 15:
          return AlertFoward;
        case 16:
          return SnapShot;
        case 17:
          return Other;
        default:
          return null;
      }
    }

    public int getValue() {
      return value;
    }

  }
}
