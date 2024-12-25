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

import java.util.Arrays;

public class LicenseJsonForStore {
  public long id;
  public byte[] license;
  public byte[] age;
  public byte[] signature;

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public byte[] getLicense() {
    return license;
  }

  public void setLicense(byte[] license) {
    this.license = license;
  }

  public byte[] getAge() {
    return age;
  }

  public void setAge(byte[] age) {
    this.age = age;
  }

  public byte[] getSignature() {
    return signature;
  }

  public void setSignature(byte[] signature) {
    this.signature = signature;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LicenseJsonForStore)) {
      return false;
    }

    LicenseJsonForStore that = (LicenseJsonForStore) o;

    if (id != that.id) {
      return false;
    }
    if (!Arrays.equals(license, that.license)) {
      return false;
    }
    if (!Arrays.equals(age, that.age)) {
      return false;
    }
    return Arrays.equals(signature, that.signature);
  }

  @Override
  public int hashCode() {
    int result = (int) (id ^ (id >>> 32));
    result = 31 * result + Arrays.hashCode(license);
    result = 31 * result + Arrays.hashCode(age);
    result = 31 * result + Arrays.hashCode(signature);
    return result;
  }
}
