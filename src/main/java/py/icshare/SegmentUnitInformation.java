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
