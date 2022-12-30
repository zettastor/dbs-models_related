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

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.sql.Blob;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import py.informationcenter.Status;
import py.informationcenter.Utils;

public class Domain {
  private Long domainId;
  private String domainName;
  private String domainDescription;
  private Set<Long> dataNodes;
  private Set<Long> storagePools;

  private AtomicLong lastUpdateTime;
  private Status status;

  private long logicalSpace;
  private long freeSpace;

  public Domain() {
    this.dataNodes = new HashSet<Long>();
    this.storagePools = new HashSet<Long>();
    this.status = Status.Available;
    this.lastUpdateTime = new AtomicLong();
  }

  public Domain(Long domainId, String domainName, String domainDescription, Set<Long> dataNodes,
      Set<Long> storagePools) {
    this.domainId = domainId;
    this.domainName = domainName;
    this.domainDescription = domainDescription;
    this.dataNodes = dataNodes;
    this.storagePools = storagePools;
    this.status = Status.Available;
    this.lastUpdateTime = new AtomicLong();
  }

  public boolean hasStoragePool() {
    if (storagePools != null && !storagePools.isEmpty()) {
      return true;
    }
    return false;
  }

  public boolean timePassedLongEnough(Long passedTimeMs) {
    return System.currentTimeMillis() - getLastUpdateTime() > passedTimeMs;
  }

  @JsonIgnore
  public boolean isDeleting() {
    return status == Status.Deleting;
  }

  public Long getDomainId() {
    return domainId;
  }

  public void setDomainId(Long domainId) {
    this.domainId = domainId;
  }

  public String getDomainName() {
    return domainName;
  }

  public void setDomainName(String domainName) {
    this.domainName = domainName;
  }

  public String getDomainDescription() {
    return domainDescription;
  }

  public void setDomainDescription(String domainDescription) {
    this.domainDescription = domainDescription;
  }

  public Set<Long> getDataNodes() {
    return dataNodes;
  }

  public void setDataNodes(Set<Long> dataNodes) {
    this.dataNodes = dataNodes;
  }

  public Set<Long> getStoragePools() {
    return this.storagePools;
  }

  public void setStoragePools(Set<Long> storagePools) {
    this.storagePools = storagePools;
  }

  public void addDatanode(Long dataNode) {
    dataNodes.add(dataNode);
  }

  public void deleteDatanode(Long dataNode) {
    dataNodes.remove(dataNode);
  }

  public void addStoragePool(Long storagePoolId) {
    storagePools.add(storagePoolId);
  }

  public void deleteStoragePool(Long storagePoolId) {
    storagePools.remove(storagePoolId);
  }

  public DomainInformationDb toDomainInformationDb(DomainDbStore domainDbStore) {
    DomainInformationDb domainInformation = new DomainInformationDb();
    if (domainId != null) {
      domainInformation.setDomainId(domainId);
    }
    if (domainName != null) {
      domainInformation.setDomainName(domainName);
    }
    if (domainDescription != null) {
      domainInformation.setDomainDescription(domainDescription);
    }
    if (dataNodes != null) {
      String dataNodesStr = Utils.bulidJsonStrFromObjectLong(dataNodes);
      Blob dataNodeBlob = domainDbStore.createBlob(dataNodesStr.getBytes());
      domainInformation.setDataNodes(dataNodeBlob);
    }
    if (storagePools != null) {
      String storagePoolsStr = Utils.bulidJsonStrFromObjectLong(storagePools);
      Blob storagePoolsBlob = domainDbStore.createBlob(storagePoolsStr.getBytes());
      domainInformation.setStoragePools(storagePoolsBlob);
    }
    if (lastUpdateTime != null) {
      domainInformation.setLastUpdateTime(getLastUpdateTime());
    }
    if (status != null) {
      domainInformation.setStatus(status.name());
    }
    return domainInformation;
  }

  public DomainInformation toDomainInformation() {
    DomainInformation domainInformation = new DomainInformation();
    if (domainId != null) {
      domainInformation.setDomainId(domainId);
    }
    if (domainName != null) {
      domainInformation.setDomainName(domainName);
    }
    if (domainDescription != null) {
      domainInformation.setDomainDescription(domainDescription);
    }
    if (dataNodes != null) {
      domainInformation.setDataNodes(Utils.bulidJsonStrFromObjectLong(dataNodes));
    }
    if (storagePools != null) {
      domainInformation.setStoragePools(Utils.bulidJsonStrFromObjectLong(storagePools));
    }
    if (lastUpdateTime != null) {
      domainInformation.setLastUpdateTime(getLastUpdateTime());
    }
    if (status != null) {
      domainInformation.setStatus(status.name());
    }
    return domainInformation;
  }

  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (obj.getClass() != getClass()) {
      return false;
    }
    Domain otherDomain = (Domain) obj;
    // domainId
    if (this.domainId == null) {
      if (otherDomain.getDomainId() != null) {
        return false;
      }
    } else if (otherDomain.getDomainId() == null) {
      return false;
    } else {
      if (!this.domainId.equals(otherDomain.getDomainId())) {
        return false;
      }
    }
    // domainName
    if (this.domainName == null) {
      if (otherDomain.getDomainName() != null) {
        return false;
      }
    } else if (otherDomain.getDomainName() == null) {
      return false;
    } else {
      if (!this.domainName.equals(otherDomain.getDomainName())) {
        return false;
      }
    }
    // domainDescription
    if (this.domainDescription == null) {
      if (otherDomain.getDomainDescription() != null) {
        return false;
      }
    } else if (otherDomain.getDomainDescription() == null) {
      return false;
    } else {
      if (!this.domainDescription.equals(otherDomain.getDomainDescription())) {
        return false;
      }
    }

    // status
    if (this.status == null) {
      if (otherDomain.getStatus() != null) {
        return false;
      }
    } else if (otherDomain.getStatus() == null) {
      return false;
    } else {
      if (!this.status.equals(otherDomain.getStatus())) {
        return false;
      }
    }

    // dataNodes
    if (this.dataNodes == null) {
      if (otherDomain.getDataNodes() != null) {
        return false;
      } else {
        return true;
      }
    } else if (otherDomain.getDataNodes() == null) {
      return false;
    } else if (this.dataNodes.size() != otherDomain.getDataNodes().size()) {
      return false;
    } else {
      int sameCount = 0;
      for (Long dataNodeInfoFromThis : this.dataNodes) {
        for (Long dataNodeInfoFromOther : otherDomain.getDataNodes()) {
          if (dataNodeInfoFromThis.equals(dataNodeInfoFromOther)) {
            sameCount++;
          }
        }
      }
      if (sameCount != this.dataNodes.size()) {
        return false;
      }
    }

    // storagePools
    if (this.storagePools == null) {
      if (otherDomain.getStoragePools() != null) {
        return false;
      } else {
        return true;
      }
    } else if (otherDomain.getStoragePools() == null) {
      return false;
    } else if (this.storagePools.size() != otherDomain.getStoragePools().size()) {
      return false;
    } else {
      int sameCount = 0;
      for (Long poolIdFromThis : this.storagePools) {
        for (Long poolIdFromOther : otherDomain.getStoragePools()) {
          if (poolIdFromThis.equals(poolIdFromOther)) {
            sameCount++;
          }
        }
      }
      if (sameCount != this.storagePools.size()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String toString() {
    return "Domain [domainId=" + domainId + ", domainName=" + domainName + ", domainDescription="
        + domainDescription + ", dataNodes=" + dataNodes + ", storagePools=" + storagePools
        + ", lastUpdateTime=" + lastUpdateTime + ", status=" + status + ", logicalSpace="
        + logicalSpace + ", freeSpace=" + freeSpace + "]";
  }

  public Long getLastUpdateTime() {
    return lastUpdateTime.get();
  }

  public void setLastUpdateTime(Long lastUpdateTime) {
    this.lastUpdateTime.set(lastUpdateTime);
  }

  public Status getStatus() {
    return status;
  }

  public void setStatus(Status status) {
    this.status = status;
  }

  public long getLogicalSpace() {
    return logicalSpace;
  }

  public void setLogicalSpace(long logicalSpace) {
    this.logicalSpace = logicalSpace;
  }

  public long getFreeSpace() {
    return freeSpace;
  }

  public void setFreeSpace(long freeSpace) {
    this.freeSpace = freeSpace;
  }
}
