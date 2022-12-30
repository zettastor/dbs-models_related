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

package py.volume;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * information about rebalance.
 */
public class VolumeRebalanceInfo {
  private static final Logger logger = LoggerFactory.getLogger(VolumeRebalanceInfo.class);

  private double rebalanceRatio = 1.0;        //rebalance progress
  private long rebalanceTotalTaskCount;       //total rebalance task count
  private long rebalanceRemainTaskCount;      //rebalance task count that not run over
  private long rebalanceVersion;              //rebalance Version(times, tell us why rebalance ratio
  // changes when volume environment changed)
  private boolean isRebalanceCalculating;     //is rebalance calculating

  public double getRebalanceRatio() {
    return rebalanceRatio;
  }

  public void setRebalanceRatio(double rebalanceRatio) {
    this.rebalanceRatio = rebalanceRatio;
  }

  public long getRebalanceTotalTaskCount() {
    return rebalanceTotalTaskCount;
  }

  public void setRebalanceTotalTaskCount(long rebalanceTotalTaskCount) {
    this.rebalanceTotalTaskCount = rebalanceTotalTaskCount;
  }

  public long getRebalanceRemainTaskCount() {
    return rebalanceRemainTaskCount;
  }

  public void setRebalanceRemainTaskCount(long rebalanceRemainTaskCount) {
    this.rebalanceRemainTaskCount = rebalanceRemainTaskCount;
  }

  public boolean isRebalanceCalculating() {
    return isRebalanceCalculating;
  }

  public void setRebalanceCalculating(boolean rebalanceCalculating) {
    isRebalanceCalculating = rebalanceCalculating;
  }

  public long getRebalanceVersion() {
    return rebalanceVersion;
  }

  public void setRebalanceVersion(long rebalanceVersion) {
    this.rebalanceVersion = rebalanceVersion;
  }

  public VolumeRebalanceInfo deepCopy(VolumeRebalanceInfo src) {
    if (src == null) {
      return null;
    }

    this.rebalanceRatio = src.getRebalanceRatio();
    this.rebalanceTotalTaskCount = src.getRebalanceTotalTaskCount();
    this.rebalanceRemainTaskCount = src.getRebalanceRemainTaskCount();
    this.rebalanceVersion = src.getRebalanceVersion();
    this.isRebalanceCalculating = src.isRebalanceCalculating();
    return this;
  }

  /**
   * calculate rebalance ratio.
   */
  public void calcRatio() {
    if (rebalanceTotalTaskCount == 0) {
      if (isRebalanceCalculating) {
        //means rebalance step is calculating
        rebalanceRatio = 0.0;
      } else {
        //means no rebalance doing
        rebalanceRatio = 1.0;
      }
    } else {
      //means has rebalance doing
      rebalanceRatio =
          (double) (rebalanceTotalTaskCount - rebalanceRemainTaskCount) / rebalanceTotalTaskCount;
    }

    if (rebalanceRatio < 0) {
      logger
          .error("remain rebalance task steps is bigger than total task steps, remain:{}, total:{}",
              rebalanceRemainTaskCount, rebalanceTotalTaskCount);
      rebalanceRatio = 0;
    }
  }

  @Override
  public String toString() {
    return "VolumeRebalanceInfo{"
        + "rebalanceRatio=" + rebalanceRatio
        + ", rebalanceTotalTaskCount=" + rebalanceTotalTaskCount
        + ", rebalanceRemainTaskCount=" + rebalanceRemainTaskCount
        + ", rebalanceVersion=" + rebalanceVersion
        + ", isRebalanceCalculating=" + isRebalanceCalculating
        + '}';
  }
}
