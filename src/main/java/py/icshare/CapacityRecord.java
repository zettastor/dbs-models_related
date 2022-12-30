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

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Blob;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.informationcenter.Utils;

public class CapacityRecord {
  public static final Long CapacityRecordID = 123456789L;
  private static final Logger logger = LoggerFactory.getLogger(CapacityRecord.class);
  private Map<String, TotalAndUsedCapacity> recordMap;

  public CapacityRecord() {
    this.recordMap = new HashMap<String, TotalAndUsedCapacity>();
  }

  public CapacityRecord(Map<String, TotalAndUsedCapacity> recordMap) {
    this.recordMap = recordMap;
  }

  public Map<String, TotalAndUsedCapacity> getRecordMap() {
    return recordMap;
  }

  public void setRecordMap(Map<String, TotalAndUsedCapacity> recordMap) {
    this.recordMap = recordMap;
  }

  public void addRecord(String date, TotalAndUsedCapacity capacityInfo) {
    recordMap.put(date, capacityInfo);
  }

  public int recordCount() {
    return recordMap.size();
  }

  public void removeEarliestRecord() {
    String earliestRecord = null;
    for (String key : recordMap.keySet()) {
      if (earliestRecord == null) {
        earliestRecord = key;
        continue;
      }
      earliestRecord = earliestRecord.compareTo(key) < 0 ? earliestRecord : key;
    }
    recordMap.remove(earliestRecord);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((recordMap == null) ? 0 : recordMap.hashCode());
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
    CapacityRecord other = (CapacityRecord) obj;
    if (recordMap == null) {
      if (other.recordMap != null) {
        return false;
      }
    } else if (!recordMap.equals(other.recordMap)) {
      return false;
    }
    return true;
  }

  public CapacityRecordInformation toCapacityRecordInformation() {
    CapacityRecordInformation recordInfo = new CapacityRecordInformation();
    recordInfo.setCapacityRecordId(CapacityRecordID);
    String capacityRecordInfo = buildWithMapForm();
    recordInfo.setCapacityRecordInfo(capacityRecordInfo);
    return recordInfo;
  }

  public CapacityRecordDbInformation toCapacityRecordDbInformation(CapacityRecordDbStore dbStore) {
    CapacityRecordDbInformation recordInfo = new CapacityRecordDbInformation();
    recordInfo.setCapacityRecordId(CapacityRecordID);
    String capacityRecordInfo = buildWithMapForm();
    byte[] capacityRecordInfoByte = null;
    if (capacityRecordInfo != null) {
      capacityRecordInfoByte = capacityRecordInfo.getBytes();
    }
    Blob capacityRecordInfoBlob = dbStore.createBlob(capacityRecordInfoByte);
    recordInfo.setCapacityRecordInfo(capacityRecordInfoBlob);
    return recordInfo;
  }

  private String buildWithMapForm() {
    StringBuilder stringBuilder = new StringBuilder();
    boolean firstLoop = true;
    for (Entry<String, TotalAndUsedCapacity> entry : recordMap.entrySet()) {
      String date = entry.getKey();
      TotalAndUsedCapacity capacityInfo = entry.getValue();
      if (firstLoop) {
        // skip the first loop to avoid append "#"
        firstLoop = false;
      } else {
        stringBuilder.append(Utils.SPLIT_CHARACTER_WITH_ENTRIES);
      }
      stringBuilder.append(date);
      stringBuilder.append(Utils.SPLIT_CHARACTER_WITH_KEY_AND_VALUE);
      ObjectMapper mapper = new ObjectMapper();
      String tmpString = null;
      try {
        mapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
        tmpString = mapper.writeValueAsString(capacityInfo);
      } catch (Exception e) {
        logger.error("map value:{}, caught an exception", capacityInfo, e);
      }
      stringBuilder.append(tmpString);
    }
    return stringBuilder.toString();
  }
}
