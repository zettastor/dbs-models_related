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

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.struct.EndPoint;
import py.informationcenter.StoragePool;
import py.instance.Group;
import py.instance.InstanceId;

/**
 * all DB tables as tight one unit in report request.
 *
 */
@Deprecated
public class ReportDbRequest {
  private static final Logger logger = LoggerFactory.getLogger(ReportDbRequest.class);
  private long sequenceId;
  private InstanceId instanceId;
  private EndPoint endPoint;
  private Group group;
  private List<Domain> domainList;
  private List<StoragePool> storagePoolList;
  private List<Volume2AccessRuleRelationship> volume2RuleList;
  private List<VolumeAccessRule> accessRuleList;
  private List<CapacityRecord> capacityRecordList;

  public ReportDbRequest() {
  }

  public ReportDbRequest(long sequenceId, InstanceId instanceId, EndPoint endPoint, Group group,
      List<Domain> domainList, List<StoragePool> storagePoolList,
      List<Volume2AccessRuleRelationship> volume2RuleList, List<VolumeAccessRule> accessRuleList,
      List<CapacityRecord> capacityRecordList) {
    this.sequenceId = sequenceId;
    this.instanceId = instanceId;
    this.endPoint = endPoint;
    this.group = group;
    this.domainList = domainList;
    this.storagePoolList = storagePoolList;
    this.volume2RuleList = volume2RuleList;
    this.accessRuleList = accessRuleList;
    this.capacityRecordList = capacityRecordList;

  }

  public void printAllList() {
    if (this.domainList != null) {
      for (Domain domain : this.domainList) {
        logger.info("domain:{}", domain);
      }
    }
    if (this.storagePoolList != null) {
      for (StoragePool storagePool : this.storagePoolList) {
        logger.info("storagePool:{}", storagePool);
      }
    }
    if (this.volume2RuleList != null) {
      for (Volume2AccessRuleRelationship volume2AccessRule : this.volume2RuleList) {
        logger.info("volume2AccessRule:{}", volume2AccessRule);
      }
    }
    if (this.accessRuleList != null) {
      for (VolumeAccessRule accessRule : this.accessRuleList) {
        logger.info("accessRule:{}", accessRule);
      }
    }
    if (this.capacityRecordList != null) {
      for (CapacityRecord capacityRecord : this.capacityRecordList) {
        logger.info("capacityRecord:{}", capacityRecord);
      }
    }
  }

  public InstanceId getInstanceId() {
    return instanceId;
  }

  public void setInstanceId(InstanceId instanceId) {
    this.instanceId = instanceId;
  }

  public EndPoint getEndPoint() {
    return endPoint;
  }

  public void setEndPoint(EndPoint endPoint) {
    this.endPoint = endPoint;
  }

  public Group getGroup() {
    return group;
  }

  public void setGroup(Group group) {
    this.group = group;
  }

  public long getSequenceId() {
    return sequenceId;
  }

  public void setSequenceId(long sequenceId) {
    this.sequenceId = sequenceId;
  }

  public List<Domain> getDomainList() {
    return domainList;
  }

  public void setDomainList(List<Domain> domainList) {
    this.domainList = domainList;
  }

  public List<StoragePool> getStoragePoolList() {
    return storagePoolList;
  }

  public void setStoragePoolList(List<StoragePool> storagePoolList) {
    this.storagePoolList = storagePoolList;
  }

  public List<Volume2AccessRuleRelationship> getVolume2RuleList() {
    return volume2RuleList;
  }

  public void setVolume2RuleList(List<Volume2AccessRuleRelationship> volume2RuleList) {
    this.volume2RuleList = volume2RuleList;
  }

  public List<VolumeAccessRule> getAccessRuleList() {
    return accessRuleList;
  }

  public void setAccessRuleList(List<VolumeAccessRule> accessRuleList) {
    this.accessRuleList = accessRuleList;
  }

  public List<CapacityRecord> getCapacityRecordList() {
    return capacityRecordList;
  }

  public void setCapacityRecordList(List<CapacityRecord> capacityRecordList) {
    this.capacityRecordList = capacityRecordList;
  }

}
