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

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.driver.DriverMetadata;
import py.driver.DriverStatus;
import py.driver.DriverType;
import py.driver.PortalType;

public class DriverInformation {
  private static final Logger logger = LoggerFactory.getLogger(DriverInformation.class);

  private DriverKeyInformation driverKeyInfo;
  private String driverName;
  private String hostName;
  private String netIfaceName;
  private String ipv6Addr;
  private String portalType;
  private int port;
  private int coordinatorPort;
  private int connectionCount;
  private String driverStatus;
  private String tagKey;
  private String tagValue;
  // driver qos: dynamic and static
  private long staticIoLimitationId;
  private long dynamicIoLimitationId;
  private int chapControl;
  private long createTime;
  private String volumeName;

  private boolean makeUnmountForCsi;

  public DriverInformation() {
  }

  public DriverInformation(long driverContainerId, long volumeId, int snapshotId,
      DriverType driverType) {
    Validate.notNull(driverType);
    driverKeyInfo = new DriverKeyInformation(driverContainerId, volumeId, snapshotId,
        driverType.name());
  }

  public String getDriverName() {
    return driverName;
  }

  public void setDriverName(String driverName) {
    this.driverName = driverName;
  }

  public DriverKeyInformation getDriverKeyInfo() {
    return driverKeyInfo;
  }

  public void setDriverKeyInfo(DriverKeyInformation driverKeyInfo) {
    this.driverKeyInfo = driverKeyInfo;
  }

  public String getHostName() {
    return hostName;
  }

  public void setHostName(String hostName) {
    this.hostName = hostName;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public int getConnectionCount() {
    return connectionCount;
  }

  public void setConnectionCount(int connectionCount) {
    this.connectionCount = connectionCount;
  }

  public String getDriverStatus() {
    return driverStatus;
  }

  public void setDriverStatus(String driverStatus) {
    this.driverStatus = driverStatus;
  }

  public String getTagKey() {
    return tagKey;
  }

  public void setTagKey(String tagKey) {
    this.tagKey = tagKey;
  }

  public String getTagValue() {
    return tagValue;
  }

  public void setTagValue(String tagValue) {
    this.tagValue = tagValue;
  }

  public int getCoordinatorPort() {
    return coordinatorPort;
  }

  public void setCoordinatorPort(int coordinatorPort) {
    this.coordinatorPort = coordinatorPort;
  }

  public long getStaticIoLimitationId() {
    return staticIoLimitationId;
  }

  public void setStaticIoLimitationId(long staticIoLimitationId) {
    this.staticIoLimitationId = staticIoLimitationId;
  }

  public long getDynamicIoLimitationId() {
    return dynamicIoLimitationId;
  }

  public void setDynamicIoLimitationId(long dynamicIoLimitationId) {
    this.dynamicIoLimitationId = dynamicIoLimitationId;
  }

  public int getChapControl() {
    return chapControl;
  }

  public void setChapControl(int chapControl) {
    this.chapControl = chapControl;
  }

  public long getCreateTime() {
    return createTime;
  }

  public void setCreateTime(long createTime) {
    this.createTime = createTime;
  }

  public String getVolumeName() {
    return volumeName;
  }

  public void setVolumeName(String volumeName) {
    this.volumeName = volumeName;
  }

  public String getNetIfaceName() {
    return netIfaceName;
  }

  public void setNetIfaceName(String netIfaceName) {
    this.netIfaceName = netIfaceName;
  }

  public String getIpv6Addr() {
    return ipv6Addr;
  }

  public void setIpv6Addr(String ipv6Addr) {
    this.ipv6Addr = ipv6Addr;
  }

  public String getPortalType() {
    return portalType;
  }

  public void setPortalType(String portalType) {
    this.portalType = portalType;
  }

  public boolean isMakeUnmountForCsi() {
    return makeUnmountForCsi;
  }

  public void setMakeUnmountForCsi(boolean makeUnmountForCsi) {
    this.makeUnmountForCsi = makeUnmountForCsi;
  }

  public DriverMetadata toDriverMetadata() {
    DriverMetadata driverMetadata = new DriverMetadata();
    driverMetadata.setDriverContainerId(driverKeyInfo.getDriverContainerId());
    driverMetadata.setDriverName(driverName);
    driverMetadata.setVolumeId(driverKeyInfo.getVolumeId());
    driverMetadata.setSnapshotId(driverKeyInfo.getSnapshotId());
    driverMetadata.setDriverType(DriverType.valueOf(driverKeyInfo.getDriverType()));
    driverMetadata.setDriverStatus(DriverStatus.findByName(driverStatus));
    driverMetadata.setHostName(hostName);
    driverMetadata.setPort(port);
    driverMetadata.setCoordinatorPort(coordinatorPort);
    driverMetadata.setLastReportTime(System.currentTimeMillis());
    driverMetadata.setDynamicIoLimitationId(dynamicIoLimitationId);
    driverMetadata.setStaticIoLimitationId(staticIoLimitationId);
    driverMetadata.setChapControl(chapControl);
    driverMetadata.setCreateTime(createTime);
    driverMetadata.setVolumeName(volumeName);
    driverMetadata.setIpv6Addr(ipv6Addr);
    driverMetadata.setPortalType(PortalType.valueOf(portalType));
    driverMetadata.setNicName(netIfaceName);
    driverMetadata.setCreateTime(createTime);
    driverMetadata.setMakeUnmountForCsi(makeUnmountForCsi);
    return driverMetadata;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((driverKeyInfo == null) ? 0 : driverKeyInfo.hashCode());
    result = prime * result + ((driverName == null) ? 0 : driverName.hashCode());
    result = prime * result + ((driverStatus == null) ? 0 : driverStatus.hashCode());
    result = prime * result + ((hostName == null) ? 0 : hostName.hashCode());
    result = prime * result + ((ipv6Addr == null) ? 0 : ipv6Addr.hashCode());
    result = prime * result + ((netIfaceName == null) ? 0 : netIfaceName.hashCode());
    result = prime * result + ((portalType == null) ? 0 : portalType.hashCode());
    result = prime * result + ((tagKey == null) ? 0 : tagKey.hashCode());
    result = prime * result + ((tagValue == null) ? 0 : tagValue.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    DriverInformation other = (DriverInformation) obj;
    if (driverKeyInfo == null) {
      if (other.driverKeyInfo != null) {
        return false;
      }
    } else if (!driverKeyInfo.equals(other.driverKeyInfo)) {
      return false;
    }
    if (driverName == null) {
      if (other.driverName != null) {
        return false;
      }
    } else if (!driverName.equals(other.driverName)) {
      return false;
    }
    if (driverStatus == null) {
      if (other.driverStatus != null) {
        return false;
      }
    } else if (!driverStatus.equals(other.driverStatus)) {
      return false;
    }
    if (hostName == null) {
      if (other.hostName != null) {
        return false;
      }
    } else if (!hostName.equals(other.hostName)) {
      return false;
    }
    if (ipv6Addr == null) {
      if (other.ipv6Addr != null) {
        return false;
      }
    } else if (!ipv6Addr.equals(other.ipv6Addr)) {
      return false;
    }
    if (netIfaceName == null) {
      if (other.netIfaceName != null) {
        return false;
      }
    } else if (!netIfaceName.equals(other.netIfaceName)) {
      return false;
    }
    if (portalType == null) {
      if (other.portalType != null) {
        return false;
      }
    } else if (!portalType.equals(other.portalType)) {
      return false;
    }
    if (tagKey == null) {
      if (other.tagKey != null) {
        return false;
      }
    } else if (!tagKey.equals(other.tagKey)) {
      return false;
    }
    if (tagValue == null) {
      if (other.tagValue != null) {
        return false;
      }
    } else if (!tagValue.equals(other.tagValue)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "DriverInformation{"
        + "driverKeyInfo=" + driverKeyInfo
        + ", driverName='" + driverName + '\''
        + ", hostName='" + hostName + '\''
        + ", netIfaceName='" + netIfaceName + '\''
        + ", ipv6Addr='" + ipv6Addr + '\''
        + ", portalType='" + portalType + '\''
        + ", port=" + port
        + ", coordinatorPort=" + coordinatorPort
        + ", connectionCount=" + connectionCount
        + ", driverStatus='" + driverStatus + '\''
        + ", tagKey='" + tagKey + '\''
        + ", tagValue='" + tagValue + '\''
        + ", staticIoLimitationId=" + staticIoLimitationId
        + ", dynamicIoLimitationId=" + dynamicIoLimitationId
        + ", chapControl=" + chapControl
        + ", createTime=" + createTime
        + ", volumeName='" + volumeName + '\''
        + ", makeUnmountForCsi=" + makeUnmountForCsi
        + '}';
  }
}
