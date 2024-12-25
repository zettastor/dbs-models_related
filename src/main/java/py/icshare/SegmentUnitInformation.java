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

import py.archive.segment.SegmentUnitStatus;

public class SegmentUnitInformation {
  private SegmentId segmentId;

  private long size;
  private int epoch;
  private int generation;
  private int status;

  public SegmentUnitInformation() {
  }

  public SegmentUnitInformation(SegmentId segmentId, long size, int epoch, int generation,
      SegmentUnitStatus status) {
    this.segmentId = segmentId;
    this.size = size;
    this.epoch = epoch;
    this.generation = generation;
    if (status != null) {
      this.status = status.getValue();
    }
  }

  public SegmentId getSegmentId() {
    return segmentId;
  }

  public void setSegmentId(SegmentId segmentId) {
    this.segmentId = segmentId;
  }

  public long getSize() {
    return size;
  }

  public void setSize(long size) {
    this.size = size;
  }

  public int getEpoch() {
    return epoch;
  }

  public void setEpoch(int epoch) {
    this.epoch = epoch;
  }

  public int getGeneration() {
    return generation;
  }

  public void setGeneration(int generation) {
    this.generation = generation;
  }

  public SegmentUnitStatus segmentUnitStatus() {
    return SegmentUnitStatus.findByValue(status);
  }

  public void segmentUnitStatus(SegmentUnitStatus status) {
    if (status != null) {
      this.status = status.getValue();
    }
  }

  public int getStatus() {
    return status;
  }

  public void setStatus(int status) {
    this.status = status;
  }

  @Override
  public String toString() {
    return "SegmentInformation [segmentId=" + segmentId + ", size=" + size + ", epoch=" + epoch
        + ", generation="
        + generation + ", status=" + status + "]";
  }
}
