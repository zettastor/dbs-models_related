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

public class PyResource {
  private long resourceId;
  private String resourceName;
  private String resourceType;

  public PyResource() {
  }

  public PyResource(long resourceId, String resourceName, String resourceType) {
    this.resourceId = resourceId;
    this.resourceName = resourceName;
    this.resourceType = resourceType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PyResource resource = (PyResource) o;
    return resourceId == resource.resourceId && Objects.equals(resourceName, resource.resourceName)
        && Objects
        .equals(resourceType, resource.resourceType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(resourceId, resourceName, resourceType);
  }

  @Override
  public String toString() {
    return "PyResource{" + "resourceId=" + resourceId + ", resourceName='" + resourceName + '\''
        + ", resourceType='" + resourceType + '\'' + '}';
  }

  public long getResourceId() {
    return resourceId;
  }

  public void setResourceId(long resourceId) {
    this.resourceId = resourceId;
  }

  public String getResourceType() {
    return resourceType;
  }

  public void setResourceType(String resourceType) {
    this.resourceType = resourceType;
  }

  public String getResourceName() {
    return resourceName;
  }

  public void setResourceName(String resourceName) {
    this.resourceName = resourceName;
  }

  public enum ResourceType {
    Volume(1),
    StoragePool(2),
    Domain(3);

    private final int value;

    ResourceType(int value) {
      this.value = value;
    }

    public static ResourceType findByValue(int value) {
      switch (value) {
        case 1:
          return Volume;
        case 2:
          return StoragePool;
        case 3:
          return Domain;
        default:
          return null;
      }
    }

    public int getValue() {
      return value;
    }
  }
}
