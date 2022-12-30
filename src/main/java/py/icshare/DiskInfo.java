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
import java.util.LinkedList;
import java.util.List;
import javax.persistence.Lob;
import org.hibernate.annotations.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.informationcenter.Utils;

public class DiskInfo {
  private static final Logger logger = LoggerFactory.getLogger(DiskInfo.class);

  private String id;
  private String sn;
  private String name;
  private String ssdOrHdd;
  private String vendor;
  private String model;
  private long rate;
  private String size;

  private String wwn;
  private String controllerId;
  private String slotNumber;
  private String enclosureId;
  private String cardType;
  private String swith; // "on", "off", "unknow"
  private String serialNumber;

  private ServerNode serverNode;

  @Lob
  @Type(type = "org.hibernate.type.BlobType")
  private Blob smartInfo;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getSn() {
    return sn;
  }

  public void setSn(String sn) {
    this.sn = sn;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getSsdOrHdd() {
    return ssdOrHdd;
  }

  public void setSsdOrHdd(String ssdOrHdd) {
    this.ssdOrHdd = ssdOrHdd;
  }

  public String getVendor() {
    return vendor;
  }

  public void setVendor(String vendor) {
    this.vendor = vendor;
  }

  public String getModel() {
    return model;
  }

  public void setModel(String model) {
    this.model = model;
  }

  public long getRate() {
    return rate;
  }

  public void setRate(long rate) {
    this.rate = rate;
  }

  public String getSize() {
    return size;
  }

  public void setSize(String size) {
    this.size = size;
  }

  public String getWwn() {
    return wwn;
  }

  public void setWwn(String wwn) {
    this.wwn = wwn;
  }

  public String getControllerId() {
    return controllerId;
  }

  public void setControllerId(String controllerId) {
    this.controllerId = controllerId;
  }

  public String getSlotNumber() {
    return slotNumber;
  }

  public void setSlotNumber(String slotNumber) {
    this.slotNumber = slotNumber;
  }

  public String getEnclosureId() {
    return enclosureId;
  }

  public void setEnclosureId(String enclosureId) {
    this.enclosureId = enclosureId;
  }

  public String getCardType() {
    return cardType;
  }

  public void setCardType(String cardType) {
    this.cardType = cardType;
  }

  public String getSwith() {
    return swith;
  }

  public void setSwith(String swith) {
    this.swith = swith;
  }

  public String getSerialNumber() {
    return serialNumber;
  }

  public void setSerialNumber(String serialNumber) {
    this.serialNumber = serialNumber;
  }

  public ServerNode getServerNode() {
    return serverNode;
  }

  public void setServerNode(ServerNode serverNode) {
    this.serverNode = serverNode;
  }

  public Blob getSmartInfo() {
    return smartInfo;
  }

  public void setSmartInfo(Blob smartInfo) {
    this.smartInfo = smartInfo;
  }

  public String smartInfoList2String(List<DiskSmartInfo> smartInfoList) throws IOException {
    String smartInfoBuf = Utils.serialize(smartInfoList);
    return smartInfoBuf;
  }

  public List<DiskSmartInfo> toSmartInfoList() {
    try {
      byte[] smartInfoBytes = py.license.Utils.readFrom(smartInfo);
      if (smartInfoBytes != null) {
        List<DiskSmartInfo> smartInfoList = (List<DiskSmartInfo>) Utils
            .deserialize(new String(smartInfoBytes));
        return smartInfoList;
      }
    } catch (SQLException | IOException | ClassNotFoundException e) {
      logger.error("caught exception when deserialize disk smart info, ", e);
    }

    return new LinkedList<>();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DiskInfo diskInfo = (DiskInfo) o;

    if (rate != diskInfo.rate) {
      return false;
    }
    if (id != null ? !id.equals(diskInfo.id) : diskInfo.id != null) {
      return false;
    }
    if (sn != null ? !sn.equals(diskInfo.sn) : diskInfo.sn != null) {
      return false;
    }
    if (name != null ? !name.equals(diskInfo.name) : diskInfo.name != null) {
      return false;
    }
    if (ssdOrHdd != null ? !ssdOrHdd.equals(diskInfo.ssdOrHdd) : diskInfo.ssdOrHdd != null) {
      return false;
    }
    if (vendor != null ? !vendor.equals(diskInfo.vendor) : diskInfo.vendor != null) {
      return false;
    }
    if (model != null ? !model.equals(diskInfo.model) : diskInfo.model != null) {
      return false;
    }
    if (size != null ? !size.equals(diskInfo.size) : diskInfo.size != null) {
      return false;
    }
    if (wwn != null ? !wwn.equals(diskInfo.wwn) : diskInfo.wwn != null) {
      return false;
    }
    if (controllerId != null ? !controllerId.equals(diskInfo.controllerId)
        : diskInfo.controllerId != null) {
      return false;
    }
    if (slotNumber != null ? !slotNumber.equals(diskInfo.slotNumber)
        : diskInfo.slotNumber != null) {
      return false;
    }
    if (enclosureId != null ? !enclosureId.equals(diskInfo.enclosureId)
        : diskInfo.enclosureId != null) {
      return false;
    }
    if (cardType != null ? !cardType.equals(diskInfo.cardType) : diskInfo.cardType != null) {
      return false;
    }
    if (swith != null ? !swith.equals(diskInfo.swith) : diskInfo.swith != null) {
      return false;
    }
    return serialNumber != null ? serialNumber.equals(diskInfo.serialNumber)
        : diskInfo.serialNumber == null;
  }

  @Override
  public int hashCode() {
    int result = id != null ? id.hashCode() : 0;
    result = 31 * result + (sn != null ? sn.hashCode() : 0);
    result = 31 * result + (name != null ? name.hashCode() : 0);
    result = 31 * result + (ssdOrHdd != null ? ssdOrHdd.hashCode() : 0);
    result = 31 * result + (vendor != null ? vendor.hashCode() : 0);
    result = 31 * result + (model != null ? model.hashCode() : 0);
    result = 31 * result + (int) (rate ^ (rate >>> 32));
    result = 31 * result + (size != null ? size.hashCode() : 0);
    result = 31 * result + (wwn != null ? wwn.hashCode() : 0);
    result = 31 * result + (controllerId != null ? controllerId.hashCode() : 0);
    result = 31 * result + (slotNumber != null ? slotNumber.hashCode() : 0);
    result = 31 * result + (enclosureId != null ? enclosureId.hashCode() : 0);
    result = 31 * result + (cardType != null ? cardType.hashCode() : 0);
    result = 31 * result + (swith != null ? swith.hashCode() : 0);
    result = 31 * result + (serialNumber != null ? serialNumber.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "DiskInfo{"
        + "id='" + id + '\''
        + ", sn='" + sn + '\''
        + ", name='" + name + '\''
        + ", ssdOrHdd='" + ssdOrHdd + '\''
        + ", vendor='" + vendor + '\''
        + ", model='" + model + '\''
        + ", rate=" + rate
        + ", size='" + size + '\''
        + ", wwn='" + wwn + '\''
        + ", controllerId='" + controllerId + '\''
        + ", slotNumber='" + slotNumber + '\''
        + ", enclosureId='" + enclosureId + '\''
        + ", cardType='" + cardType + '\''
        + ", swith='" + swith + '\''
        + ", serialNumber='" + serialNumber + '\''
        + ", serverNode=" + serverNode.getId()
        + '}';
  }
}
