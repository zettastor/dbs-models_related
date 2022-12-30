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
