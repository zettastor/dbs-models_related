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
