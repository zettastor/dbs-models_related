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
import py.thrift.share.CapacityRecordThrift;
import py.thrift.share.DomainThrift;
import py.thrift.share.StoragePoolThrift;
import py.thrift.share.VolumeAccessRuleThrift;
import py.thrift.share.VolumeRuleRelationshipThrift;

/**
 * all DB tables as tight one unit in report response.
 *
 */
@Deprecated
public class ReportDbResponse {
  private Long sequenceId;

  private List<DomainThrift> domainList;
  private List<StoragePoolThrift> storagePoolList;
  private List<VolumeRuleRelationshipThrift> volume2RuleList;
  private List<VolumeAccessRuleThrift> accessRuleList;
  private List<CapacityRecordThrift> capacityRecordList;

  public ReportDbResponse(Long sequenceId) {
    this.sequenceId = sequenceId;
  }

  public Long getSequenceId() {
    return sequenceId;
  }

  public void setSequenceId(Long sequenceId) {
    this.sequenceId = sequenceId;
  }

  public List<DomainThrift> getDomainList() {
    return domainList;
  }

  public void setDomainList(List<DomainThrift> domainList) {
    this.domainList = domainList;
  }

  public List<StoragePoolThrift> getStoragePoolList() {
    return storagePoolList;
  }

  public void setStoragePoolList(List<StoragePoolThrift> storagePoolList) {
    this.storagePoolList = storagePoolList;
  }

  public List<VolumeRuleRelationshipThrift> getVolume2RuleList() {
    return volume2RuleList;
  }

  public void setVolume2RuleList(List<VolumeRuleRelationshipThrift> volume2RuleList) {
    this.volume2RuleList = volume2RuleList;
  }

  public List<VolumeAccessRuleThrift> getAccessRuleList() {
    return accessRuleList;
  }

  public void setAccessRuleList(List<VolumeAccessRuleThrift> accessRuleList) {
    this.accessRuleList = accessRuleList;
  }

  public List<CapacityRecordThrift> getCapacityRecordList() {
    return capacityRecordList;
  }

  public void setCapacityRecordList(List<CapacityRecordThrift> capacityRecordList) {
    this.capacityRecordList = capacityRecordList;
  }

}
