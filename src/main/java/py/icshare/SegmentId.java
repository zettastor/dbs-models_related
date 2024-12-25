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

import java.io.Serializable;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class SegmentId implements Serializable {
  private static final long serialVersionUID = 1L;

  private long volumeId;
  private int index;
  private long instanceId;

  public SegmentId() {
  }

  public SegmentId(long volumeId, int index, long instanceId) {
    this.volumeId = volumeId;
    this.index = index;
    this.instanceId = instanceId;
  }

  public long getVolumeId() {
    return volumeId;
  }

  public void setVolumeId(long volumeId) {
    this.volumeId = volumeId;
  }

  public int getIndex() {
    return index;
  }

  public void setIndex(int index) {
    this.index = index;
  }

  public boolean equals(Object obj) {
    if (!(obj instanceof SegmentId)) {
      return false;
    }

    SegmentId tmp = (SegmentId) obj;
    if (this.index == tmp.index && this.volumeId == tmp.volumeId
        && this.instanceId == tmp.instanceId) {
      return true;
    }

    return false;
  }

  public int hashCode() {
    return new HashCodeBuilder(-528253723, -475504089).appendSuper(super.hashCode())
        .append(this.volumeId)
        .append(this.index).toHashCode();
  }

  @Override
  public String toString() {
    return "SegmentId [volumeId=" + volumeId + ", index=" + index + ", instanceId=" + instanceId
        + "]";
  }

  public long getInstanceId() {
    return instanceId;
  }

  public void setInstanceId(long instanceId) {
    this.instanceId = instanceId;
  }

}
