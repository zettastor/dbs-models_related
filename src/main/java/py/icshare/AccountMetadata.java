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

import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.codec.digest.DigestUtils;
import py.common.RequestIdBuilder;
import py.icshare.authorization.PyResource;
import py.icshare.authorization.Role;

public class AccountMetadata {
  private Long accountId;
  private String accountName;
  private String hashedPassword;

  private String salt;
  private String accountType;

  private Set<Role> roles;
  private Set<PyResource> resources;

  private Date createdAt;

  public AccountMetadata() {
  }

  public AccountMetadata(String accountName, String password, String accountType, long accountId) {
    this.accountName = accountName;
    this.salt = UUID.randomUUID().toString();
    this.hashedPassword = genHashedPassword(password, salt);
    this.accountType = accountType;
    this.createdAt = new Date();
    this.accountId = accountId;
    this.roles = new HashSet<>();
    this.resources = new HashSet<>();
  }

  private static String genHashedPassword(String password, String salt) {
    return DigestUtils.sha256Hex(password + salt);
  }

  public static long randomId() {
    return RequestIdBuilder.get();
  }

  public Long getAccountId() {
    return accountId;
  }

  public void setAccountId(Long accountId) {
    this.accountId = accountId;
  }

  public String getAccountName() {
    return accountName;
  }

  public void setAccountName(String accountName) {
    this.accountName = accountName;
  }

  public String getHashedPassword() {
    return hashedPassword;
  }

  public void setHashedPassword(String hashedPassword) {
    this.hashedPassword = hashedPassword;
  }

  public String getSalt() {
    return salt;
  }

  public void setSalt(String salt) {
    this.salt = salt;
  }

  public String getAccountType() {
    return accountType;
  }

  public void setAccountType(String accountType) {
    this.accountType = accountType;
  }

  public boolean passwordMatch(String password) {
    return this.hashedPassword.equals(genHashedPassword(password, this.salt));
  }

  public Date getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(Date createdAt) {
    this.createdAt = createdAt;
  }

  @Override
  public String toString() {
    return "AccountMetadata [accountId=" + accountId + ", accountName=" + accountName + "]";
  }

  public Set<Role> getRoles() {
    return roles;
  }

  public void setRoles(Set<Role> roles) {
    this.roles = roles;
  }

  public Set<PyResource> getResources() {
    return resources;
  }

  public void setResources(Set<PyResource> resources) {
    this.resources = resources;
  }

  public enum AccountType {
    SuperAdmin(1),
    Admin(2),
    Regular(3);

    private final int value;

    private AccountType(int value) {
      this.value = value;
    }

    public static AccountType findByValue(int value) {
      switch (value) {
        case 1:
          return SuperAdmin;
        case 2:
          return Admin;
        case 3:
          return Regular;
        default:
          return null;
      }
    }

    public int getValue() {
      return value;
    }
  }
}
