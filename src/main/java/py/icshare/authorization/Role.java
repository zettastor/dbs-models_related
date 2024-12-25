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

package py.icshare.authorization;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import py.common.RequestIdBuilder;

public class Role {
  private Long id;

  private String name;

  private String description;

  private Set<ApiToAuthorize> permissions;

  private boolean builtIn;

  private boolean superAdmin;

  public Role() {
    this.permissions = new HashSet<>();
  }

  public Role(Long id, String name) {
    this.id = id;
    this.name = name;
    this.permissions = new HashSet<>();
    this.builtIn = false;
    this.superAdmin = false;
  }

  public Role(Long id, String name, boolean builtIn) {
    this.id = id;
    this.name = name;
    this.permissions = new HashSet<>();
    this.builtIn = builtIn;
    this.superAdmin = false;
  }

  public Role(String name, String description, Set<ApiToAuthorize> permissions, boolean builtIn,
      boolean superAdmin) {
    this.id = RequestIdBuilder.get();
    this.name = name;
    this.description = description;
    this.permissions = permissions;
    this.builtIn = builtIn;
    this.superAdmin = superAdmin;
  }

  public Role(Long id, String name, boolean builtIn, boolean superAdmin) {
    this.id = id;
    this.name = name;
    this.permissions = new HashSet<>();
    this.builtIn = builtIn;
    this.superAdmin = superAdmin;
  }

  public boolean addPermission(ApiToAuthorize api) {
    return permissions.add(api);
  }

  @Override
  public String toString() {
    return "Role [RoleId=" + id + ", RoleName=" + name + ", Description=" + description
        + ", Permission=" + permissions + "]";
  }

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public Set<ApiToAuthorize> getPermissions() {
    return permissions;
  }

  public void setPermissions(Set<ApiToAuthorize> permissions) {
    this.permissions = permissions;
  }

  public boolean isBuiltIn() {
    return builtIn;
  }

  public void setBuiltIn(boolean builtIn) {
    this.builtIn = builtIn;
  }

  public boolean isSuperAdmin() {
    return superAdmin;
  }

  public void setSuperAdmin(boolean superAdmin) {
    this.superAdmin = superAdmin;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Role role = (Role) o;
    return Objects.equals(id, role.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }
}
