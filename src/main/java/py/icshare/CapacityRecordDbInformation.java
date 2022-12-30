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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.Map;
import javax.persistence.Lob;
import org.apache.commons.lang3.Validate;
import org.hibernate.annotations.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.informationcenter.Utils;

public class CapacityRecordDbInformation {
  private static final Logger logger = LoggerFactory.getLogger(CapacityRecordDbInformation.class);
  private Long capacityRecordId;

  @Lob
  @Type(type = "org.hibernate.type.BlobType")
  private Blob capacityRecordInfo;

  public Blob getCapacityRecordInfo() {
    return capacityRecordInfo;
  }

  public void setCapacityRecordInfo(Blob capacityRecordInfo) {
    this.capacityRecordInfo = capacityRecordInfo;
  }

  public Long getCapacityRecordId() {
    return capacityRecordId;
  }

  public void setCapacityRecordId(Long capacityRecordId) {
    this.capacityRecordId = capacityRecordId;
  }

  public CapacityRecord toCapacityRecord() throws Exception {
    CapacityRecord capacityRecord = new CapacityRecord();
    // step1: transition blob to string
    String capacityRecordInfoStr = null;
    try {
      byte[] capacityRecordInfoByte = py.license.Utils.readFrom(capacityRecordInfo);
      if (capacityRecordInfoByte != null) {
        capacityRecordInfoStr = new String(capacityRecordInfoByte);
      }
    } catch (SQLException | IOException e1) {
      logger.error("caught an exception, when transite blob to string");
      throw e1;
    }
    logger.debug("after transition capacityRecordInfoStr : {}", capacityRecordInfoStr);
    if (capacityRecordInfoStr == null) {
      logger.warn("capacityRecordInfoStr is null");
      return capacityRecord;
    }

    // step 2: find all entries
    String[] stringArray = capacityRecordInfoStr.split(Utils.SPLIT_CHARACTER_WITH_ENTRIES);
    // step 3: parse all key and values and fill into map
    Map<String, TotalAndUsedCapacity> mapInfo = capacityRecord.getRecordMap();
    for (int i = 0; i < stringArray.length; i++) {
      String keyAndValue = stringArray[i];
      String[] keyAndValueArray = keyAndValue.split(Utils.SPLIT_CHARACTER_WITH_KEY_AND_VALUE);
      Validate.isTrue(keyAndValueArray.length == 2, "must contain element %s", keyAndValue);
      String date = keyAndValueArray[0];
      ObjectMapper mapper = new ObjectMapper();
      TotalAndUsedCapacity capacityInfo = null;
      try {
        capacityInfo = mapper.readValue(keyAndValueArray[1], TotalAndUsedCapacity.class);
      } catch (Exception e) {
        logger.error("read value:{}, caught an exception", keyAndValueArray[1], e);
      }
      mapInfo.put(date, capacityInfo);
    }
    return capacityRecord;
  }

  @Override
  public String toString() {
    return "CapacityRecordInformation [capacityRecordID=" + capacityRecordId
        + ", capacityRecordInfo="
        + capacityRecordInfo + "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((capacityRecordId == null) ? 0 : capacityRecordId.hashCode());
    result = prime * result + ((capacityRecordInfo == null) ? 0 : capacityRecordInfo.hashCode());
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
    CapacityRecordDbInformation other = (CapacityRecordDbInformation) obj;
    if (capacityRecordId == null) {
      if (other.capacityRecordId != null) {
        return false;
      }
    } else if (!capacityRecordId.equals(other.capacityRecordId)) {
      return false;
    }
    if (capacityRecordInfo == null) {
      if (other.capacityRecordInfo != null) {
        return false;
      }
    } else if (!capacityRecordInfo.equals(other.capacityRecordInfo)) {
      return false;
    }
    return true;
  }
}
