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

import java.io.Serializable;

/**
 * about disk smart column info.
 */
public class DiskSmartInfo implements Serializable {
  private static final long serialVersionUID = 1L;

  private String id;
  private String attributeNameEn;
  private String attributeNameCn;
  private String flag;
  private String value;
  private String worst;
  private String thresh;
  private String type;
  private String updated;
  private String whenFailed;
  private String rawValue;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getAttributeNameEn() {
    return attributeNameEn;
  }

  public void setAttributeNameEn(String attributeNameEn) {
    this.attributeNameEn = attributeNameEn;
  }

  public String getAttributeNameCn() {
    return attributeNameCn;
  }

  public void setAttributeNameCn(String attributeNameCn) {
    this.attributeNameCn = attributeNameCn;
  }

  public String getFlag() {
    return flag;
  }

  public void setFlag(String flag) {
    this.flag = flag;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  public String getWorst() {
    return worst;
  }

  public void setWorst(String worst) {
    this.worst = worst;
  }

  public String getThresh() {
    return thresh;
  }

  public void setThresh(String thresh) {
    this.thresh = thresh;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getUpdated() {
    return updated;
  }

  public void setUpdated(String updated) {
    this.updated = updated;
  }

  public String getWhenFailed() {
    return whenFailed;
  }

  public void setWhenFailed(String whenFailed) {
    this.whenFailed = whenFailed;
  }

  public String getRawValue() {
    return rawValue;
  }

  public void setRawValue(String rawValue) {
    this.rawValue = rawValue;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DiskSmartInfo that = (DiskSmartInfo) o;

    if (id != null ? !id.equals(that.id) : that.id != null) {
      return false;
    }
    if (attributeNameEn != null ? !attributeNameEn.equals(that.attributeNameEn)
        : that.attributeNameEn != null) {
      return false;
    }
    if (attributeNameCn != null ? !attributeNameCn.equals(that.attributeNameCn)
        : that.attributeNameCn != null) {
      return false;
    }
    if (flag != null ? !flag.equals(that.flag) : that.flag != null) {
      return false;
    }
    if (value != null ? !value.equals(that.value) : that.value != null) {
      return false;
    }
    if (worst != null ? !worst.equals(that.worst) : that.worst != null) {
      return false;
    }
    if (thresh != null ? !thresh.equals(that.thresh) : that.thresh != null) {
      return false;
    }
    if (type != null ? !type.equals(that.type) : that.type != null) {
      return false;
    }
    if (updated != null ? !updated.equals(that.updated) : that.updated != null) {
      return false;
    }
    if (whenFailed != null ? !whenFailed.equals(that.whenFailed) : that.whenFailed != null) {
      return false;
    }
    return rawValue != null ? rawValue.equals(that.rawValue) : that.rawValue == null;
  }

  @Override
  public int hashCode() {
    int result = id != null ? id.hashCode() : 0;
    result = 31 * result + (attributeNameEn != null ? attributeNameEn.hashCode() : 0);
    result = 31 * result + (attributeNameCn != null ? attributeNameCn.hashCode() : 0);
    result = 31 * result + (flag != null ? flag.hashCode() : 0);
    result = 31 * result + (value != null ? value.hashCode() : 0);
    result = 31 * result + (worst != null ? worst.hashCode() : 0);
    result = 31 * result + (thresh != null ? thresh.hashCode() : 0);
    result = 31 * result + (type != null ? type.hashCode() : 0);
    result = 31 * result + (updated != null ? updated.hashCode() : 0);
    result = 31 * result + (whenFailed != null ? whenFailed.hashCode() : 0);
    result = 31 * result + (rawValue != null ? rawValue.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "DiskSmartInfo{"
        + "id='" + id + '\''
        + ", attributeNameEn='" + attributeNameEn + '\''
        + ", attributeNameCn='" + attributeNameCn + '\''
        + ", flag='" + flag + '\''
        + ", value='" + value + '\''
        + ", worst='" + worst + '\''
        + ", thresh='" + thresh + '\''
        + ", type='" + type + '\''
        + ", updated='" + updated + '\''
        + ", whenFailed='" + whenFailed + '\''
        + ", rawValue='" + rawValue + '\''
        + '}';
  }
}
