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

package py.archive.segment;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Placehold for volume version.
 *
 * <p>version includes two parts, the epoch number, and generation numbers.
 *
 * <p>Every time when a new leader is elected for this segment, the epoch number is increased by 1.
 * Every time when a segment member joins/drop out the segment, the generation number is increased
 * by 1.
 */
public class SegmentVersion implements Comparable<SegmentVersion> {
  private static final int INIT_GENERATION = 0;
  private static final int INIT_EPOCH = 0;
  private static final SegmentVersion INIT_SEGMENT_VERSION = new SegmentVersion(INIT_EPOCH,
      INIT_GENERATION);
  private static Logger logger = LoggerFactory.getLogger(SegmentVersion.class);
  private final int epoch;
  private final int generation;

  public SegmentVersion(@JsonProperty("epoch") int epoch,
      @JsonProperty("generation") int generation) {
    Validate.isTrue(epoch >= 0 && generation >= 0,
        "Epoch and generation must be positive. Use INIT_SEGMENT_VERSION if you want to use 0,0");
    this.epoch = epoch;
    this.generation = generation;
  }

  public SegmentVersion(SegmentVersion copyFrom) {
    this.epoch = copyFrom.epoch;
    this.generation = copyFrom.generation;
  }

  public SegmentVersion incEpoch() {
    return new SegmentVersion(epoch + 1, INIT_GENERATION);
  }

  public SegmentVersion incGeneration() {
    return new SegmentVersion(epoch, generation + 1);
  }

  public int getEpoch() {
    return epoch;
  }

  public int getGeneration() {
    return generation;
  }

  @Override
  public String toString() {
    return "SegmentVersion(epoch=" + epoch + ", generation=" + generation + ")";
  }

  @Override
  public int compareTo(SegmentVersion o) {
    if (epoch > o.epoch) {
      return 1;
    } else if (epoch < o.epoch) {
      return -1;
    } else if (generation > o.generation) {
      return 1;
    } else if (generation == o.generation) {
      return 0;
    } else {
      return -1;
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + epoch;
    result = prime * result + generation;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof SegmentVersion)) {
      return false;
    }

    if (this == obj) {
      return true;
    }
    SegmentVersion other = (SegmentVersion) obj;
    if (epoch != other.epoch) {
      return false;
    }
    if (generation != other.generation) {
      return false;
    }
    return true;
  }
}
