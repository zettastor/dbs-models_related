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
