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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import javax.persistence.Lob;
import net.sf.json.JSONArray;
import org.hibernate.annotations.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerNode {
  private static final Logger logger = LoggerFactory.getLogger(ServerNode.class);
  private String id;
  private String hostName;
  private String modelInfo;
  private String cpuInfo;
  private String memoryInfo;
  private String diskInfo;
  private String networkCardInfo;
  private String networkCardInfoName;
  private String manageIp;
  private String gatewayIp;
  private String storeIp;
  private String rackNo;
  private String slotNo;
  private String status; // ok, unknown
  private String childFramNo;
  private Set<DiskInfo> diskInfoSet;
  private InstanceMetadata.DatanodeStatus datanodeStatus;

  @Lob
  @Type(type = "org.hibernate.type.BlobType")
  private Blob sensorInfo;

  public Set<SensorInfo> getSensorInfoList() {
    byte[] sensorInfoBytes = new byte[0];
    try {
      sensorInfoBytes = py.license.Utils.readFrom(sensorInfo);
      if (sensorInfoBytes != null) {
        String sensorInfoStr = new String(sensorInfoBytes);
        logger.warn("sensorInfoStr is {} ", sensorInfoStr);
        Set<SensorInfo> sensorInfoSet = new HashSet<>();
        ObjectMapper objectMapper = new ObjectMapper();
        TypeReference<Set<SensorInfo>> typeRef = new TypeReference<Set<SensorInfo>>() {
        };
        sensorInfoSet = objectMapper.readValue(sensorInfoStr, typeRef);
        return sensorInfoSet;
      }
    } catch (SQLException | IOException e) {
      logger.error("caught exception when get sensor info, ", e);
    }
    return new HashSet<>();
  }

  public String sensorInfoSet2String(Set<SensorInfo> sensorInfoSet) throws IOException {
    if (sensorInfoSet == null) {
      logger.warn("Don't have any sensorInfoSet to convert");
    }
    JSONArray array = JSONArray.fromObject(sensorInfoSet);
    return array.toString();
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getHostName() {
    return hostName;
  }

  public void setHostName(String hostName) {
    this.hostName = hostName;
  }

  public String getModelInfo() {
    return modelInfo;
  }

  public void setModelInfo(String modelInfo) {
    this.modelInfo = modelInfo;
  }

  public String getCpuInfo() {
    return cpuInfo;
  }

  public void setCpuInfo(String cpuInfo) {
    this.cpuInfo = cpuInfo;
  }

  public String getMemoryInfo() {
    return memoryInfo;
  }

  public void setMemoryInfo(String memoryInfo) {
    this.memoryInfo = memoryInfo;
  }

  public String getDiskInfo() {
    return diskInfo;
  }

  public void setDiskInfo(String diskInfo) {
    this.diskInfo = diskInfo;
  }

  public String getNetworkCardInfo() {
    return networkCardInfo;
  }

  public void setNetworkCardInfo(String networkCardInfo) {
    this.networkCardInfo = networkCardInfo;
  }

  public String getManageIp() {
    return manageIp;
  }

  public void setManageIp(String manageIp) {
    this.manageIp = manageIp;
  }

  public String getGatewayIp() {
    return gatewayIp;
  }

  public void setGatewayIp(String gatewayIp) {
    this.gatewayIp = gatewayIp;
  }

  public String getStoreIp() {
    return storeIp;
  }

  public void setStoreIp(String storeIp) {
    this.storeIp = storeIp;
  }

  public String getRackNo() {
    return rackNo;
  }

  public void setRackNo(String rackNo) {
    this.rackNo = rackNo;
  }

  public String getSlotNo() {
    return slotNo;
  }

  public void setSlotNo(String slotNo) {
    this.slotNo = slotNo;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public Set<DiskInfo> getDiskInfoSet() {
    return diskInfoSet;
  }

  public void setDiskInfoSet(Set<DiskInfo> diskInfoSet) {
    this.diskInfoSet = diskInfoSet;
  }

  public String getNetworkCardInfoName() {
    return networkCardInfoName;
  }

  public void setNetworkCardInfoName(String networkCardInfoName) {
    this.networkCardInfoName = networkCardInfoName;
  }

  public String getChildFramNo() {
    return childFramNo;
  }

  public void setChildFramNo(String childFramNo) {
    this.childFramNo = childFramNo;
  }

  public InstanceMetadata.DatanodeStatus getDatanodeStatus() {
    return datanodeStatus;
  }

  public void setDatanodeStatus(InstanceMetadata.DatanodeStatus datanodeStatus) {
    this.datanodeStatus = datanodeStatus;
  }

  public Blob getSensorInfo() {
    return sensorInfo;
  }

  public void setSensorInfo(Blob sensorInfo) {
    this.sensorInfo = sensorInfo;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ServerNode that = (ServerNode) o;

    if (id != null ? !id.equals(that.id) : that.id != null) {
      return false;
    }
    if (hostName != null ? !hostName.equals(that.hostName) : that.hostName != null) {
      return false;
    }
    if (modelInfo != null ? !modelInfo.equals(that.modelInfo) : that.modelInfo != null) {
      return false;
    }
    if (cpuInfo != null ? !cpuInfo.equals(that.cpuInfo) : that.cpuInfo != null) {
      return false;
    }
    if (memoryInfo != null ? !memoryInfo.equals(that.memoryInfo) : that.memoryInfo != null) {
      return false;
    }
    if (diskInfo != null ? !diskInfo.equals(that.diskInfo) : that.diskInfo != null) {
      return false;
    }
    if (networkCardInfo != null ? !networkCardInfo.equals(that.networkCardInfo)
        : that.networkCardInfo != null) {
      return false;
    }
    if (manageIp != null ? !manageIp.equals(that.manageIp) : that.manageIp != null) {
      return false;
    }
    if (gatewayIp != null ? !gatewayIp.equals(that.gatewayIp) : that.gatewayIp != null) {
      return false;
    }
    if (storeIp != null ? !storeIp.equals(that.storeIp) : that.storeIp != null) {
      return false;
    }
    if (rackNo != null ? !rackNo.equals(that.rackNo) : that.rackNo != null) {
      return false;
    }
    if (slotNo != null ? !slotNo.equals(that.slotNo) : that.slotNo != null) {
      return false;
    }
    return diskInfoSet != null ? diskInfoSet.equals(that.diskInfoSet) : that.diskInfoSet == null;
  }

  @Override
  public int hashCode() {
    int result = id != null ? id.hashCode() : 0;
    result = 31 * result + (hostName != null ? hostName.hashCode() : 0);
    result = 31 * result + (modelInfo != null ? modelInfo.hashCode() : 0);
    result = 31 * result + (cpuInfo != null ? cpuInfo.hashCode() : 0);
    result = 31 * result + (memoryInfo != null ? memoryInfo.hashCode() : 0);
    result = 31 * result + (diskInfo != null ? diskInfo.hashCode() : 0);
    result = 31 * result + (networkCardInfo != null ? networkCardInfo.hashCode() : 0);
    result = 31 * result + (networkCardInfoName != null ? networkCardInfoName.hashCode() : 0);
    result = 31 * result + (manageIp != null ? manageIp.hashCode() : 0);
    result = 31 * result + (gatewayIp != null ? gatewayIp.hashCode() : 0);
    result = 31 * result + (storeIp != null ? storeIp.hashCode() : 0);
    result = 31 * result + (rackNo != null ? rackNo.hashCode() : 0);
    result = 31 * result + (slotNo != null ? slotNo.hashCode() : 0);
    result = 31 * result + (status != null ? status.hashCode() : 0);
    result = 31 * result + (childFramNo != null ? childFramNo.hashCode() : 0);
    result = 31 * result + (diskInfoSet != null ? diskInfoSet.hashCode() : 0);
    result = 31 * result + (datanodeStatus != null ? datanodeStatus.hashCode() : 0);
    result = 31 * result + (sensorInfo != null ? sensorInfo.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "ServerNode{" + "id='" + id + '\'' + ", hostName='" + hostName + '\'' + ", modelInfo='"
        + modelInfo
        + '\'' + ", cpuInfo='" + cpuInfo + '\'' + ", memoryInfo='" + memoryInfo + '\''
        + ", diskInfo='"
        + diskInfo + '\'' + ", networkCardInfo='" + networkCardInfo + '\''
        + ", networkCardInfoName='"
        + networkCardInfoName + '\'' + ", manageIp='" + manageIp + '\'' + ", gatewayIp='"
        + gatewayIp + '\''
        + ", storeIp='" + storeIp + '\'' + ", rackNo='" + rackNo + '\'' + ", slotNo='" + slotNo
        + '\''
        + ", status='" + status + '\'' + ", childFramNo='" + childFramNo + '\'' + ", diskInfoSet="
        + diskInfoSet
        + ", datanodeStatus=" + datanodeStatus + ", sensorInfo=" + sensorInfo + '}';
  }
}
