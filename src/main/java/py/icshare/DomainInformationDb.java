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

import java.io.IOException;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import javax.persistence.Lob;
import org.hibernate.annotations.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.informationcenter.Status;
import py.informationcenter.Utils;

public class DomainInformationDb {
  private static final Logger logger = LoggerFactory.getLogger(DomainInformationDb.class);
  private Long domainId;
  private String domainName;
  private String domainDescription;

  @Lob
  @Type(type = "org.hibernate.type.BlobType")
  private Blob dataNodes;

  @Lob
  @Type(type = "org.hibernate.type.BlobType")
  private Blob storagePools;

  private String status;
  private Long lastUpdateTime;

  public DomainInformationDb() {
  }

  public DomainInformationDb(Long domainId, String domainName, String domainDescription,
      Blob dataNodes,
      Blob storagePools) {
    this.domainId = domainId;
    this.domainName = domainName;
    this.domainDescription = domainDescription;
    this.dataNodes = dataNodes;
    this.storagePools = storagePools;
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

  public Blob getDataNodes() {
    return dataNodes;
  }

  public void setDataNodes(Blob dataNodes) {
    this.dataNodes = dataNodes;
  }

  public Blob getStoragePools() {
    return storagePools;
  }

  public void setStoragePools(Blob storagePools) {
    this.storagePools = storagePools;
  }

  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (obj.getClass() != getClass()) {
      return true;
    }

    DomainInformationDb otherDomainInformation = (DomainInformationDb) obj;
    // domainId
    if (this.domainId == null) {
      if (otherDomainInformation.getDomainId() != null) {
        return false;
      }
    } else {
      if (!this.domainId.equals(otherDomainInformation.getDomainId())) {
        return false;
      }
    }
    // dataNodes
    if (this.dataNodes == null) {
      if (otherDomainInformation.getDataNodes() != null) {
        return false;
      }
    } else {
      if (!this.dataNodes.equals(otherDomainInformation.getDataNodes())) {
        return false;
      }
    }

    // storagePools
    if (this.storagePools == null) {
      if (otherDomainInformation.getStoragePools() != null) {
        return false;
      }
    } else {
      if (!this.storagePools.equals(otherDomainInformation.getStoragePools())) {
        return false;
      }
    }

    // domainName
    if (this.domainName == null) {
      if (otherDomainInformation.getDomainName() != null) {
        return false;
      }
    } else {
      if (!this.domainName.equals(otherDomainInformation.getDomainName())) {
        return false;
      }
    }
    // domainDescription
    if (this.domainDescription == null) {
      if (otherDomainInformation.getDomainDescription() != null) {
        return false;
      }
    } else {
      if (!this.domainDescription.equals(otherDomainInformation.getDomainDescription())) {
        return false;
      }
    }
    return true;
  }

  public Domain toDomain() throws SQLException, IOException {
    Domain domain = new Domain();
    if (domainId != null) {
      domain.setDomainId(domainId);
    }
    if (domainName != null) {
      domain.setDomainName(domainName);
    }
    if (domainDescription != null) {
      domain.setDomainDescription(domainDescription);
    }
    if (dataNodes != null) {
      String dataNodesStr = new String(py.license.Utils.readFrom(dataNodes));
      Set<Long> datanodeList = new HashSet<Long>();
      datanodeList.addAll(Utils.parseObjecLongFromJsonStr(dataNodesStr));
      if (datanodeList != null) {
        domain.setDataNodes(datanodeList);
      }
    }
    if (storagePools != null) {
      String storagePoolsStr = new String(py.license.Utils.readFrom(storagePools));
      Set<Long> storagePoolSet = new HashSet<Long>();
      storagePoolSet.addAll(Utils.parseObjecLongFromJsonStr(storagePoolsStr));
      if (storagePools != null) {
        domain.setStoragePools(storagePoolSet);
      }
    }
    if (lastUpdateTime != null) {
      domain.setLastUpdateTime(lastUpdateTime);
    }
    if (status != null) {
      domain.setStatus(Status.findByName(status));
    }
    return domain;
  }

  @Override
  public String toString() {
    return "DomainInformation [domainId=" + domainId + ", domainName=" + domainName
        + ", domainDescription="
        + domainDescription + ", dataNodes=" + dataNodes + ", storagePools=" + storagePools
        + ", status="
        + status + ", lastUpdateTime=" + lastUpdateTime + "]";
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public Long getLastUpdateTime() {
    return lastUpdateTime;
  }

  public void setLastUpdateTime(Long lastUpdateTime) {
    this.lastUpdateTime = lastUpdateTime;
  }

}
